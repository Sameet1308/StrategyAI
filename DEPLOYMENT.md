# StrategyAI BOT — Codebase Guide & AWS Deployment Blueprint

> **Audience: an AI coding agent (or engineer) onboarding to this repo.**
> This single document explains what the system is, how the code is organized,
> the target AWS architecture, and a resource-by-resource Terraform blueprint to
> replicate it in your own AWS account. It is written in generic terms — fill in
> the `<placeholders>` with your organization's values.

---

## 1. How to use this document

1. Read §2–§4 to understand the app and its runtime shape.
2. Use §5 (container) + §7 (Terraform blueprint) to provision infrastructure.
   Generate one `.tf` module per subsection; the HCL snippets are starting
   points, not complete files — adapt names, CIDRs, and IDs to your account.
3. Use §6 (config mapping) to wire the app's environment variables to the
   provisioned resources (IRSA, Secrets Manager, ConfigMap).
4. Use §8 as the deploy runbook and §9 as the platform-team checklist.

Ground truth for behavior is the code itself; ground truth for the MSTR REST
calls is [`backend/MSTR_API_NOTES.md`](backend/MSTR_API_NOTES.md).

---

## 2. What the system is

**StrategyAI BOT** is an internal admin copilot for a MicroStrategy (Strategy
One) environment. An architect or LOB admin types a natural-language request;
the backend uses Claude (tool-use) to detect intent, resolves object names to
IDs, validates a typed payload, and — for any state-changing action — renders a
preview and **waits for explicit human confirmation** before calling the MSTR
REST API. Every step is written to an audit log keyed to the real user.

**Core safety contract (do not weaken):** the LLM proposes tool calls;
deterministic code validates them against JSON Schema and gates every mutating
call behind a user confirmation. There is no code path from an LLM response to a
mutating MSTR call that skips confirmation.

v1 scope: subscription management + intelligent-cube operations (25 tools).

---

## 3. Codebase map

```
StrategyAI/
├── backend/
│   ├── app/
│   │   ├── main.py            # FastAPI factory; serves API + built React SPA;
│   │   │                      #   build_executor()/build_llm() pick mock vs live
│   │   ├── config.py          # ALL config from env (.env). Mode switches:
│   │   │                      #   STRATEGYAI_MOCK_MSTR, STRATEGYAI_LLM_PROVIDER
│   │   ├── db.py, models.py    # SQLAlchemy: conversations, messages,
│   │   │                      #   pending_actions (TTL), audit_log
│   │   ├── identity.py        # user from ALB OIDC header (x-amzn-oidc-data),
│   │   │                      #   dev fallback header locally
│   │   ├── schemas.py         # Pydantic request/response models
│   │   ├── api/routes.py      # /api/chat, /api/actions/{id}/confirm, /api/audit
│   │   ├── agent/
│   │   │   ├── loop.py        # THE agent loop + confirmation gate (see §2)
│   │   │   ├── registry.py    # 25 tools: name + JSON Schema + preview + mutating
│   │   │   ├── prompts.py     # system prompt (ask-don't-guess contract)
│   │   │   └── llm.py         # LLM adapters: Bedrock | Anthropic API | Mock
│   │   └── mstr/
│   │       ├── client.py      # authenticated MSTR REST session (login, retry)
│   │       ├── executors.py   # RealMstrExecutor — verified live endpoints
│   │       ├── mock.py        # MockMstrExecutor — seeded offline data
│   │       └── errors.py      # MstrApiError
│   ├── tests/                 # 74 pytest tests (loop, gate, wire fidelity)
│   ├── validate_live.py       # read-only live endpoint validator (safe on prod)
│   ├── requirements.txt
│   └── MSTR_API_NOTES.md      # verified MSTR REST endpoint reference
├── frontend/                  # Vite + React chat UI → builds into backend/static
├── .env.example               # mock vs live recipe
├── CLAUDE.md                  # project guide + run/test commands
└── DEPLOYMENT.md              # this file
```

**Extension pattern:** a new capability = one `ToolSpec` in `registry.py` + one
`_<tool>()` method in both `executors.py` (live) and `mock.py` (offline) + tests.
No re-architecture.

---

## 4. Target AWS architecture

Design principles enforced by this architecture (confirm each against your
platform standards — see §9):

- **Internal-only.** No public internet surface. No CloudFront, no public API
  Gateway, no Cognito. The only ingress is an **internal Application Load
  Balancer** that performs **OIDC authentication** against the corporate IdP.
- **Containers on EKS with EC2 managed node groups** (not Fargate).
- **The React SPA ships inside the FastAPI container** (FastAPI serves the static
  bundle) — no separate static hosting.
- **MicroStrategy runs in a separate (vendor) AWS account**, reached over
  **PrivateLink** — not the public internet.
- **A single MSTR admin service account** is used for REST calls; per-user
  identity is preserved in the app's own audit log.

```
Corporate network
      │  (OIDC login)
      ▼
[ Internal ALB ]───authenticate-oidc──▶ corporate IdP (OIDC)
      │
      ▼
┌─────────────────────── EKS (EC2 node groups) ───────────────────────┐
│  FastAPI pod   : React SPA + REST API + agent loop + confirm gate    │
│  Worker pod    : drains SQS, calls MSTR REST, writes job_status      │
│  CronJob pod   : scheduled/nightly tasks (optional, "trends" track)  │
└──────┬──────────────┬───────────────────┬───────────────────┬───────┘
       │              │                   │                   │
       ▼              ▼                   ▼                   ▼
  Amazon Bedrock   Amazon SQS        Amazon RDS          PrivateLink ─▶ MSTR
  (Claude +        (job queue)       PostgreSQL:            (vendor AWS,
   Titan Embed)                       audit · chats ·        egress via
                                      pgvector · sessions ·  central acct +
                                      job_status             firewall/IPS)

Cross-cutting (mandatory):
  CloudWatch (metrics/logs/alarms)
      └─▶ SSM Parameter Store  "circuit-breaker" kill-switch (SecureString)
      └─▶ Bedrock Guardrails   (content + action policy)
  Secrets Manager (MSTR creds, DB creds, signing keys) · KMS CMKs · ECR
```

**The circuit breaker** is a runtime kill-switch: a SecureString flag in SSM
Parameter Store that the request path reads *before* executing any action. When
tripped (automatically on a sustained CloudWatch anomaly, or manually by
on-call) the agent returns a fallback message instead of acting — at
single-agent, agent-category, or whole-subsystem granularity, with no redeploy.
See §10 for how to wire it into `loop.py` (it is designed but not yet in code).

---

## 5. Container packaging

One image serves API + SPA. Build the React bundle into `backend/static`, then
package the Python app.

```dockerfile
# ---- build the React SPA ----
FROM node:22-alpine AS web
WORKDIR /web
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build            # outputs to ../backend/static

# ---- runtime ----
FROM python:3.12-slim
WORKDIR /app
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/ ./
COPY --from=web /backend/static ./static
EXPOSE 8000
# non-root
RUN useradd -m appuser && chown -R appuser /app
USER appuser
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

The Worker and CronJob workloads run the same image with a different command
(e.g. a `worker.py` entrypoint that drains SQS). Push the image to **ECR**.

---

## 6. Configuration mapping (env → AWS)

The app reads everything from environment variables (see `config.py`). Map them:

| Env var | Source in AWS | Notes |
|---|---|---|
| `STRATEGYAI_MOCK_MSTR` | ConfigMap = `false` | live mode |
| `STRATEGYAI_LLM_PROVIDER` | ConfigMap = `bedrock` | in-AWS |
| `AWS_REGION` | ConfigMap | Bedrock region |
| `BEDROCK_MODEL_ID` | ConfigMap | e.g. an approved Claude model id |
| `MSTR_BASE_URL` | ConfigMap | `https://<mstr-privatelink-dns>/MicroStrategyLibrary/api` |
| `MSTR_USERNAME` / `MSTR_PASSWORD` | **Secrets Manager** → K8s Secret | service account |
| `STRATEGYAI_DATABASE_URL` | **Secrets Manager** → K8s Secret | RDS Postgres URL |
| `STRATEGYAI_REQUIRE_ALB_AUTH` | ConfigMap = `true` | enforce OIDC header |
| Bedrock/SQS/Secrets access | **IRSA** (IAM role for service account) | no static keys |

Bedrock, SQS, Secrets Manager, and SSM access are granted via **IRSA** — the pod
assumes an IAM role; do not mount static AWS keys.

---

## 7. Terraform blueprint

Suggested module layout (`terraform/`):

```
terraform/
├── main.tf              # providers, remote state
├── network.tf           # VPC, subnets, endpoints, PrivateLink to MSTR
├── eks.tf               # cluster, node groups, IRSA, ALB controller
├── data.tf              # RDS Postgres, SQS
├── ai.tf                # Bedrock IAM, Guardrails
├── security.tf          # Secrets Manager, KMS, SSM circuit-breaker param
├── observability.tf     # CloudWatch log groups, alarms
├── ecr.tf               # container registry
└── variables.tf
```

### 7.0 Providers & state
```hcl
terraform {
  required_version = ">= 1.6"
  required_providers { aws = { source = "hashicorp/aws", version = "~> 5.0" } }
  backend "s3" {                     # use your approved state bucket + lock table
    bucket = "<tf-state-bucket>"
    key    = "strategyai/terraform.tfstate"
    region = "<region>"
    dynamodb_table = "<tf-lock-table>"
    encrypt = true
  }
}
provider "aws" { region = var.region }
```

### 7.1 Network (`network.tf`)
- Reuse an existing VPC if your platform team provides one; otherwise a VPC with
  **private subnets only** across ≥2 AZs (this workload has no public subnet
  requirement).
- **Interface VPC endpoints** (keep AWS traffic off the internet):
  `bedrock-runtime`, `secretsmanager`, `ssm`, `sqs`, `ecr.api`, `ecr.dkr`,
  `logs`, `sts`, `elasticloadbalancing`. **Gateway endpoint** for `s3`.
- **PrivateLink to MSTR:** an interface endpoint to the MSTR service (the MSTR
  vendor account exposes an endpoint service name — get it from that team):
```hcl
resource "aws_vpc_endpoint" "mstr" {
  vpc_id              = var.vpc_id
  service_name        = var.mstr_endpoint_service_name   # from the MSTR account
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.mstr_egress.id]
  private_dns_enabled = true
}
```
- Security groups: pods egress to the MSTR endpoint SG on 443 only; ALB→pods on
  the app port; pods→RDS on 5432.

### 7.2 EKS + IRSA + ALB controller (`eks.tf`)
Use `terraform-aws-modules/eks/aws`. EC2 **managed node groups** (no Fargate).
```hcl
module "eks" {
  source          = "terraform-aws-modules/eks/aws"
  version         = "~> 20.0"
  cluster_name    = "strategyai"
  cluster_version = "1.30"
  vpc_id          = var.vpc_id
  subnet_ids      = var.private_subnet_ids
  cluster_endpoint_public_access = false          # private cluster
  eks_managed_node_groups = {
    default = { instance_types = ["t3.large"], min_size = 2, max_size = 4, desired_size = 2 }
  }
  enable_irsa = true
}
```
- Install the **AWS Load Balancer Controller** (Helm) so a Kubernetes `Ingress`
  provisions the **internal** ALB.
- **IRSA role** for the app service account with least-privilege policy:
  `bedrock:InvokeModel` (+ `InvokeModelWithResponseStream`), `sqs:*Message*` on
  the queue, `secretsmanager:GetSecretValue` on named secrets, `ssm:GetParameter`
  on the circuit-breaker path, `kms:Decrypt` on the CMKs, `logs:PutLogEvents`.

Ingress (internal ALB + OIDC) is expressed as annotations on the K8s Ingress,
not pure Terraform:
```yaml
alb.ingress.kubernetes.io/scheme: internal
alb.ingress.kubernetes.io/target-type: ip
alb.ingress.kubernetes.io/listen-ports: '[{"HTTPS":443}]'
alb.ingress.kubernetes.io/auth-type: oidc
alb.ingress.kubernetes.io/auth-idp-oidc: '{"issuer":"https://<idp>/","authorizationEndpoint":"...","tokenEndpoint":"...","userInfoEndpoint":"...","secretName":"oidc-client"}'
```

### 7.3 RDS Postgres + SQS (`data.tf`)
```hcl
resource "aws_db_instance" "app" {
  identifier            = "strategyai"
  engine                = "postgres"
  engine_version        = "16"
  instance_class        = "db.t4g.medium"          # size to your load
  allocated_storage     = 50
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.data.arn
  multi_az              = true
  db_subnet_group_name  = aws_db_subnet_group.app.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  username              = "strategyai"
  manage_master_user_password = true               # stored in Secrets Manager
  deletion_protection   = true
}
resource "aws_sqs_queue" "jobs" {
  name                       = "strategyai-jobs"
  visibility_timeout_seconds = 300
  kms_master_key_id          = aws_kms_key.data.arn
}
```
Enable the **`vector` (pgvector)** extension in the DB for the Phase-2B knowledge
base. `job_status`, `sessions`, `audit`, and `chats` are all tables in this one
instance (a dedicated cache service is intentionally not used).

### 7.4 Bedrock + Guardrails (`ai.tf`)
- No resource provisions the model; access is the IRSA policy in §7.2.
- Create a **Bedrock Guardrail** (`aws_bedrock_guardrail`) for content + denied
  topics and reference its id/version from the app config.

### 7.5 Security: Secrets, KMS, circuit breaker (`security.tf`)
```hcl
resource "aws_kms_key" "data"    { description = "StrategyAI data at rest"  enable_key_rotation = true }
resource "aws_kms_key" "secrets" { description = "StrategyAI secrets"       enable_key_rotation = true }

resource "aws_secretsmanager_secret" "mstr" { name = "strategyai/mstr" kms_key_id = aws_kms_key.secrets.arn }
# value: {"username":"...","password":"..."} — set out-of-band, not in TF

# Circuit-breaker kill-switch (see §10). SecureString, read at request time.
resource "aws_ssm_parameter" "breaker_global" {
  name  = "/strategyai/circuit-breaker/global"
  type  = "SecureString"
  value = "closed"                                  # "open" trips it
  key_id = aws_kms_key.secrets.arn
  lifecycle { ignore_changes = [value] }            # flipped operationally
}
```

### 7.6 Observability (`observability.tf`)
- CloudWatch log group per workload; retention per policy.
- Alarms on error rate / latency / tool-failure count that (optionally) drive an
  automation to flip the circuit-breaker parameter to `open`.

### 7.7 ECR (`ecr.tf`)
```hcl
resource "aws_ecr_repository" "app" {
  name                 = "strategyai"
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "KMS" kms_key = aws_kms_key.data.arn }
}
```

---

## 8. Deploy runbook

1. `terraform apply` the modules in §7 (network → eks → data → security → ai → ecr).
2. Populate Secrets Manager: MSTR service-account creds, DB URL/creds.
3. Build & push the image (§5) to ECR.
4. Install the AWS Load Balancer Controller; apply K8s manifests:
   Deployment (FastAPI), Deployment (Worker), CronJob, Service, Ingress
   (internal + OIDC annotations), ConfigMap, ExternalSecret/Secret, and the
   ServiceAccount annotated with the IRSA role ARN.
5. **Validate the live MSTR calls from inside the cluster** before opening it up:
   `kubectl exec` into the FastAPI pod and run `python backend/validate_live.py`.
   Fix any endpoint the validator flags (it prints the exact call + MSTR error).
6. Smoke-test one Bedrock call and one confirmed mutating action end-to-end.

---

## 9. Confirm with your platform team (generic constraints)

These are assumptions baked into the architecture — verify each:

- Internal-only ingress is acceptable (no public endpoint needed).
- EKS with EC2 node groups is the approved compute (not Fargate).
- The corporate IdP supports OIDC on the ALB (issuer/client/secret available).
- The MSTR account exposes a **PrivateLink endpoint service**; get its name and
  the DNS to use for `MSTR_BASE_URL`.
- Egress from your account to the MSTR endpoint is allowed through the central
  egress/firewall path.
- Approved model id(s) for Bedrock and whether Guardrails are mandatory.
- State bucket, KMS policy, tagging, and naming standards.

---

## 10. Circuit breaker — wiring it into the code

Designed in the architecture, not yet in `loop.py`. To implement:

1. Add a `circuit_breaker.py` that reads the SSM parameter(s) with a short cache
   (e.g. 10 s TTL) — global, per-agent-category, and per-tool paths under
   `/strategyai/circuit-breaker/`.
2. In `AgentLoop.handle_chat`, **after** intent detection and **before**
   proposing/executing any tool, check the breaker for that tool's category. If
   open, return a fallback `AgentReply(kind="error", ...)`, write an audit row,
   and (optionally) emit a CloudWatch metric / notify on-call.
3. Local/dev: back it with an env var or a DB row so the same code runs offline.

This adds a global emergency stop on top of the existing per-action confirmation
gate — two independent safety layers.

---

*This document contains no organization-specific names, hostnames, or account
identifiers by design. Replace every `<placeholder>` with your values.*
