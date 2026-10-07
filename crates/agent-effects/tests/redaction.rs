//! Redaction: secrets never reach the store.

use std::sync::{Arc, Mutex};

use agent_effects::redaction::{Field, REDACTED};
use agent_effects::store::EffectStore;
use agent_effects::{
    ApprovalDecision, ApprovalProvider, ApprovalRequest, EffectFailure, EffectKey, EffectName,
    EffectOutcome, LogicalKey, RedactKeys, Resolution, Runtime, RuntimeError, Secret,
};
use agent_effects_memory::MemoryStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Serialize)]
struct Charge {
    account: String,
    card_token: Secret<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Session {
    user: String,
    access_token: Secret<String>,
}

fn key(name: &str, logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new(name).unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

/// Every value the store holds about an effect, as one JSON document.
async fn everything_stored(store: &MemoryStore, key: &EffectKey) -> String {
    let record = store.get_by_key(key).await.unwrap().unwrap();
    let events = store.events(record.id).await.unwrap();
    serde_json::to_string(&(record, events)).unwrap()
}

#[tokio::test]
async fn a_secret_input_is_stored_redacted_but_the_action_gets_it() {
    let store = MemoryStore::new();
    let rt = Runtime::new(store.clone());
    let token = Secret::new("tok_live_123".to_string());
    let charge = Charge {
        account: "acct_1".into(),
        card_token: token.clone(),
    };
    let outcome = rt
        .effect("payment.charge", "order-1")
        .input(&charge)
        .run(move |_| {
            let token = token.clone();
            async move {
                assert_eq!(token.expose().map(String::as_str), Some("tok_live_123"));
                Ok::<_, EffectFailure>(())
            }
        })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed(()));
    let record = store
        .get_by_key(&key("payment.charge", "order-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.input,
        Some(json!({ "account": "acct_1", "card_token": REDACTED }))
    );
    assert!(
        !everything_stored(&store, &record.key)
            .await
            .contains("tok_live_123")
    );
}

#[tokio::test]
async fn secrets_are_not_part_of_an_effects_identity() {
    let rt = Runtime::new(MemoryStore::new());
    let charge = |account: &str, token: &str| {
        let charge = Charge {
            account: account.into(),
            card_token: Secret::new(token.into()),
        };
        rt.effect("payment.charge", "order-1")
            .input(&charge)
            .run(|_| async { Ok::<_, EffectFailure>(()) })
    };
    charge("acct_1", "tok_a").await.unwrap();
    // A rotated token is the same effect: its value is never stored, so it
    // cannot be compared.
    assert_eq!(
        charge("acct_1", "tok_b").await.unwrap(),
        EffectOutcome::Committed(())
    );
    // A different account is not.
    let err = charge("acct_2", "tok_a").await.unwrap_err();
    assert!(matches!(err, RuntimeError::InputMismatch { .. }), "{err}");
}

#[tokio::test]
async fn a_secret_output_reaches_its_caller_but_replays_redacted() {
    let store = MemoryStore::new();
    let rt = Runtime::new(store.clone());
    let login = || {
        rt.effect("auth.login", "ada").run(|_| async {
            Ok::<_, EffectFailure>(Session {
                user: "ada".into(),
                access_token: Secret::new("eyJ.secret".into()),
            })
        })
    };
    let EffectOutcome::Committed(fresh) = login().await.unwrap() else {
        panic!("expected a commit");
    };
    assert_eq!(
        fresh.access_token.expose().map(String::as_str),
        Some("eyJ.secret")
    );

    let EffectOutcome::Committed(replayed) = login().await.unwrap() else {
        panic!("expected a replay");
    };
    assert_eq!(replayed.user, "ada");
    assert!(
        replayed.access_token.is_redacted(),
        "the token was never stored"
    );
    assert!(
        !everything_stored(&store, &key("auth.login", "ada"))
            .await
            .contains("eyJ.secret")
    );
}

#[tokio::test]
async fn a_redactor_masks_inputs_outputs_and_operator_notes() {
    let store = MemoryStore::new();
    let rt = Runtime::builder(store.clone())
        .redactor(RedactKeys::new(["card_number", "note"]))
        .build();
    let outcome = rt
        .effect("card.add", "ada")
        .input(&json!({ "user": "ada", "card_number": "4242424242424242" }))
        .run(|_| async { Err::<Value, _>(EffectFailure::ambiguous("timed out")) })
        .await
        .unwrap();
    let EffectOutcome::NeedsIntervention { id } = outcome else {
        panic!("{outcome:?}");
    };
    rt.resolve(
        id,
        Resolution::applied(&json!({ "card_number": "4242424242424242", "last4": "4242" }))
            .unwrap(),
        "operator:dennis",
        "card 4242424242424242 is on file",
    )
    .await
    .unwrap();

    let stored = everything_stored(&store, &key("card.add", "ada")).await;
    assert!(!stored.contains("4242424242424242"), "{stored}");
    let record = store
        .get_by_key(&key("card.add", "ada"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.input,
        Some(json!({ "user": "ada", "card_number": REDACTED }))
    );
    assert_eq!(
        record.output,
        Some(json!({ "card_number": REDACTED, "last4": "4242" }))
    );
}

#[tokio::test]
async fn error_messages_are_redacted_before_they_are_stored() {
    let store = MemoryStore::new();
    let rt = Runtime::builder(store.clone())
        .redactor(|field: Field, _: &EffectName, value: &mut Value| {
            if field == Field::ErrorMessage
                && let Some(text) = value.as_str()
            {
                *value = Value::String(text.replace("sk_live_1", REDACTED));
            }
        })
        .build();
    let outcome = rt
        .effect("payment.charge", "order-2")
        .run(|_| async { Err::<(), _>(EffectFailure::permanent("invalid api key sk_live_1")) })
        .await
        .unwrap();
    let EffectOutcome::Failed(error) = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(error.message, "invalid api key [REDACTED]");
    assert!(
        !everything_stored(&store, &key("payment.charge", "order-2"))
            .await
            .contains("sk_live_1")
    );
}

/// Records the input approvers are shown.
#[derive(Clone, Default)]
struct Shown(Arc<Mutex<Vec<Option<Value>>>>);

impl ApprovalProvider for Shown {
    fn request(
        &self,
        request: ApprovalRequest,
    ) -> impl std::future::Future<Output = ApprovalDecision> + Send {
        self.0.lock().unwrap().push(request.input);
        std::future::ready(ApprovalDecision::Approved { by: "alice".into() })
    }
}

#[tokio::test]
async fn approvers_see_only_the_redacted_input() {
    let shown = Shown::default();
    let rt = Runtime::builder(MemoryStore::new())
        .approval_provider(shown.clone())
        .build();
    let charge = Charge {
        account: "acct_1".into(),
        card_token: Secret::new("tok_live_123".into()),
    };
    rt.effect("payment.charge", "order-3")
        .input(&charge)
        .require_approval()
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap();
    assert_eq!(
        *shown.0.lock().unwrap(),
        [Some(json!({ "account": "acct_1", "card_token": REDACTED }))]
    );
}

#[tokio::test]
async fn redacted_fields_are_never_hashed_into_the_identity() {
    let rt = Runtime::builder(MemoryStore::new())
        .redactor(RedactKeys::new(["card_number"]))
        .build();
    let add = |card: &str| {
        rt.effect("card.add", "ada")
            .input(&json!({ "user": "ada", "card_number": card }))
            .run(|_| async { Ok::<_, EffectFailure>(()) })
    };
    add("4242424242424242").await.unwrap();
    // Had the fingerprint been taken before redaction, this would be an
    // input mismatch, and the store would hold a hash of the card number.
    assert_eq!(
        add("5555555555554444").await.unwrap(),
        EffectOutcome::Committed(())
    );
}

#[tokio::test]
async fn an_actions_output_is_redacted_in_storage_but_not_for_its_caller() {
    let store = MemoryStore::new();
    let rt = Runtime::builder(store.clone())
        .redactor(RedactKeys::new(["api_key"]))
        .build();
    let create = || {
        rt.effect("key.create", "svc-1").run(|_| async {
            Ok::<_, EffectFailure>(json!({ "id": "key_1", "api_key": "sk_live_new" }))
        })
    };
    assert_eq!(
        create().await.unwrap(),
        EffectOutcome::Committed(json!({ "id": "key_1", "api_key": "sk_live_new" })),
        "the caller that created it gets the key"
    );
    assert_eq!(
        create().await.unwrap(),
        EffectOutcome::Committed(json!({ "id": "key_1", "api_key": REDACTED })),
        "a replay gets what was stored"
    );
    assert!(
        !everything_stored(&store, &key("key.create", "svc-1"))
            .await
            .contains("sk_live_new")
    );
}
