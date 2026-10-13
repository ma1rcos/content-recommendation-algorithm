//! Identidade do solicitante.
//!
//! O `user_id` do path NUNCA autoriza nada sozinho: com `AUTH_MODE=hmac`, o
//! cliente apresenta `Authorization: Bearer <token>`, e o token é
//! `u.<user_id>.<exp>.<hmac>` (usuário) ou `a.0.<exp>.<hmac>` (admin), com
//! `hmac = hex(HMAC-SHA256(AUTH_SECRET, "<tipo>.<id>.<exp>"))`.
//! Em produção isso seria substituído por OIDC/JWT de um provedor de identidade;
//! o formato aqui é deliberadamente simples para a demonstração
//! (gerador: scripts/make_token.py).

use actix_web::{FromRequest, HttpRequest, dev::Payload, web};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::future::{Ready, ready};

use crate::{config::AuthMode, error::ApiError, state::AppState};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Identity {
    User(i64),
    Admin,
    /// AUTH_MODE=disabled (somente desenvolvimento).
    Unauthenticated,
}

impl Identity {
    pub fn can_act_as(&self, user_id: i64) -> bool {
        match self {
            Identity::User(id) => *id == user_id,
            Identity::Admin | Identity::Unauthenticated => true,
        }
    }

    pub fn is_admin(&self) -> bool {
        matches!(self, Identity::Admin | Identity::Unauthenticated)
    }

    /// Chave estável para rate limiting.
    pub fn rate_key(&self) -> String {
        match self {
            Identity::User(id) => format!("u{id}"),
            Identity::Admin => "admin".into(),
            Identity::Unauthenticated => "anon".into(),
        }
    }

    pub fn require_user(&self, user_id: i64) -> Result<(), ApiError> {
        if self.can_act_as(user_id) {
            Ok(())
        } else {
            Err(ApiError::Forbidden)
        }
    }

    pub fn require_admin(&self) -> Result<(), ApiError> {
        if self.is_admin() {
            Ok(())
        } else {
            Err(ApiError::Forbidden)
        }
    }
}

fn signature(secret: &[u8], payload: &str) -> Vec<u8> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).expect("HMAC aceita qualquer tamanho de chave");
    mac.update(payload.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

pub fn sign(secret: &str, kind: &str, id: i64, exp: i64) -> String {
    let payload = format!("{kind}.{id}.{exp}");
    format!(
        "{payload}.{}",
        hex::encode(signature(secret.as_bytes(), &payload))
    )
}

pub fn verify(secret: &str, token: &str, now: i64) -> Result<Identity, ApiError> {
    let mut parts = token.splitn(4, '.');
    let (Some(kind), Some(id), Some(exp), Some(sig)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(ApiError::Unauthorized);
    };
    let id: i64 = id.parse().map_err(|_| ApiError::Unauthorized)?;
    let exp: i64 = exp.parse().map_err(|_| ApiError::Unauthorized)?;
    let sig = hex::decode(sig).map_err(|_| ApiError::Unauthorized)?;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_| ApiError::Internal)?;
    mac.update(format!("{kind}.{id}.{exp}").as_bytes());
    // verify_slice compara em tempo constante.
    mac.verify_slice(&sig).map_err(|_| ApiError::Unauthorized)?;
    if exp < now {
        return Err(ApiError::Unauthorized);
    }
    match (kind, id) {
        ("u", id) if id > 0 => Ok(Identity::User(id)),
        ("a", 0) => Ok(Identity::Admin),
        _ => Err(ApiError::Unauthorized),
    }
}

impl FromRequest for Identity {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let Some(state) = req.app_data::<web::Data<AppState>>() else {
            return ready(Err(ApiError::Internal));
        };
        if state.cfg.auth_mode == AuthMode::Disabled {
            return ready(Ok(Identity::Unauthenticated));
        }
        let token = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        ready(match token {
            Some(t) => verify(
                &state.cfg.auth_secret,
                t.trim(),
                chrono::Utc::now().timestamp(),
            ),
            None => Err(ApiError::Unauthorized),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn valid_user_token() {
        let t = sign(SECRET, "u", 42, 2_000);
        assert_eq!(verify(SECRET, &t, 1_000).unwrap(), Identity::User(42));
    }

    #[test]
    fn rejects_tampering_expiry_and_wrong_secret() {
        let t = sign(SECRET, "u", 42, 2_000);
        let forged = t.replacen("u.42.", "u.43.", 1);
        assert!(verify(SECRET, &forged, 1_000).is_err());
        assert!(verify(SECRET, &t, 3_000).is_err());
        assert!(verify("another-secret-another-secret-xx", &t, 1_000).is_err());
        assert!(verify(SECRET, "garbage", 1_000).is_err());
    }

    #[test]
    fn admin_token_and_permissions() {
        let t = sign(SECRET, "a", 0, 2_000);
        let id = verify(SECRET, &t, 1_000).unwrap();
        assert!(id.is_admin() && id.can_act_as(7));
        let user = Identity::User(7);
        assert!(user.can_act_as(7) && !user.can_act_as(8) && !user.is_admin());
    }
}
