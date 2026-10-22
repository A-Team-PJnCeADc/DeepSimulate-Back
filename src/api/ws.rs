use crate::api::AppState;
use crate::error::{AppError, DomainError};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/ws", get(ws_upgrade))
}

#[derive(Deserialize)]
struct TokenQuery {
    token: Option<String>,
}

async fn ws_upgrade(
    State(st): State<Arc<AppState>>,
    Query(q): Query<TokenQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let token = q.token.ok_or(DomainError::Unauthorized)?;
    let session = st.session_of(&token).ok_or(DomainError::Unauthorized)?;
    Ok(ws.on_upgrade(move |socket| handle(socket, st, session)))
}

#[derive(Deserialize)]
struct ClientMsg {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    codes: Vec<String>,
}

async fn handle(socket: WebSocket, st: Arc<AppState>, session: crate::api::Session) {
    let user_id = session.user_id;
    let (mut sink, mut stream) = socket.split();
    let (cid, mut out) = st.core.ws.register(user_id);

    st.core.ws.send_to_client(
        cid,
        json!({
            "type": "ready",
            "data": { "user_id": user_id, "username": session.username }
        }),
    );
    if let Some(view) = st.core.account_view(user_id) {
        st.core.ws.push_account(&view);
    }

    let writer = tokio::spawn(async move {
        while let Some(msg) = out.recv().await {
            if sink
                .send(Message::Text(msg.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut subs: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            frame = stream.next() => {
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        let parsed: Result<ClientMsg, _> = serde_json::from_str(&text);
                        let Ok(msg) = parsed else { continue };
                        match msg.kind.as_str() {
                            "subscribe" => {
                                let added: Vec<String> = msg.codes.iter()
                                    .filter(|c| !subs.contains(*c))
                                    .cloned()
                                    .collect();
                                if !added.is_empty() {
                                    st.market.subscribe(&added);
                                    for c in added { subs.insert(c); }
                                    st.core.ws.set_subs(cid, subs.clone());
                                }
                                st.core.ws.send_to_client(cid, json!({
                                    "type": "subscribed",
                                    "data": { "codes": subs.iter().cloned().collect::<Vec<_>>() }
                                }));
                            }
                            "unsubscribe" => {
                                let removed: Vec<String> = msg.codes.iter()
                                    .filter(|c| subs.contains(*c))
                                    .cloned()
                                    .collect();
                                if !removed.is_empty() {
                                    st.market.unsubscribe(&removed);
                                    for c in &removed { subs.remove(c); }
                                    st.core.ws.set_subs(cid, subs.clone());
                                }
                            }
                            "ping" => {
                                st.core.ws.send_to_client(cid, json!({ "type": "pong", "data": Value::Null }));
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }

    let codes: Vec<String> = subs.into_iter().collect();
    st.market.unsubscribe(&codes);
    st.core.ws.unregister(cid);
    writer.abort();
}
