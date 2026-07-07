"""Resolve the acting user.

Production: the internal ALB authenticates against Okta (OIDC) and forwards
the identity as the `x-amzn-oidc-data` JWT header. The ALB is the trust
boundary — the pod's security group only accepts traffic from the ALB, so the
header cannot be spoofed from outside. We parse the claims here; signature
verification against the ALB regional public key can be layered on later
without changing callers.

Local dev: no ALB, so fall back to the X-Dev-User header or a configured
default identity.
"""

import base64
import binascii
import json

from fastapi import HTTPException, Request

from .config import settings

_ALB_HEADER = "x-amzn-oidc-data"


def _decode_jwt_claims(token: str) -> dict:
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_b64))
    except (IndexError, ValueError, binascii.Error, json.JSONDecodeError):
        raise HTTPException(status_code=401, detail="Invalid identity token")


def get_current_user(request: Request) -> str:
    token = request.headers.get(_ALB_HEADER)
    if token:
        claims = _decode_jwt_claims(token)
        user = claims.get("email") or claims.get("preferred_username") or claims.get("sub")
        if not user:
            raise HTTPException(status_code=401, detail="Identity token has no subject")
        return user

    if settings.require_alb_auth:
        raise HTTPException(status_code=401, detail="Missing identity header")

    return request.headers.get("X-Dev-User") or settings.dev_user
