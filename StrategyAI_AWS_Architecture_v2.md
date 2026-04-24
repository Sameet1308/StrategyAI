# StrategyAI — AWS Architecture (Revised)

**Target system:** MicroStrategy (MSTR) in vendor network
**Auth:** Okta + Azure AD · **Compute mandate:** EKS only (no Fargate)
**MSTR connectivity:** AWS PrivateLink (already in place between enterprise ↔ vendor)

---

## Diagram A — Icon/Connector View (mermaid `architecture-beta`)

```mermaid
architecture-beta
    group aws(cloud)[Enterprise AWS Account]

    group edge[Edge Public] in aws
    service cf(internet)[CloudFront CDN] in edge
    service s3web(disk)[S3 React Bundle] in edge
    service waf(server)[AWS WAF] in edge
    service apigw(server)[API Gateway] in edge

    group vpc[VPC Private Subnets] in aws
    service alb(server)[Internal ALB] in vpc
    service api(server)[EKS FastAPI Pods] in vpc
    service wrk(server)[EKS Worker Pods] in vpc
    service sqs(server)[SQS Job Queue] in vpc
    service rds(database)[RDS PostgreSQL] in vpc
    service redis(database)[ElastiCache Redis] in vpc
    service sm(disk)[Secrets Manager] in vpc
    service pl(server)[PrivateLink Endpoint] in vpc

    service user(internet)[Enterprise User]
    service okta(server)[Okta + Azure AD]
    service bv(server)[BlueVerse LLM]
    service mstr(database)[MSTR Vendor Network]

    user:R --> L:cf
    cf:B --> T:s3web
    cf:R --> L:waf
    user:B --> T:apigw
    apigw:R --> L:okta
    apigw:B --> T:alb
    alb:R --> L:api
    api:R --> L:sqs
    sqs:R --> L:wrk
    api:B --> T:rds
    wrk:B --> T:redis
    api:T --> B:bv
    wrk:R --> L:pl
    pl:R --> L:mstr
    api:B --> T:sm
```

---

## Diagram B — Detailed Flow View (mermaid `flowchart`)

```mermaid
flowchart LR
    User((Enterprise User))
    Okta[[Okta + Azure AD]]
    BV[[BlueVerse Foundry LLM]]
    MSTR[(MSTR - Vendor Network)]

    subgraph AWS["Enterprise AWS Account"]
        direction TB

        subgraph Edge["Edge - Public"]
            WAF[AWS WAF]
            CF[CloudFront]
            S3S[(S3 React Bundle)]
            APIGW[API Gateway<br/>Okta JWT authorizer]
        end

        subgraph VPC["VPC - Private Subnets"]
            ALB[Internal ALB]

            subgraph EKSBox["EKS Cluster - EC2 Node Groups"]
                API[FastAPI Pods<br/>intent + RBAC]
                WRK[Worker Pods<br/>poll MSTR jobs]
            end

            SQS[[SQS Queue]]
            RDS[(RDS PostgreSQL<br/>audit + pgvector RAG)]
            REDIS[(ElastiCache Redis<br/>session + status)]
            SM[Secrets Manager]
            PL[PrivateLink Endpoint<br/>to vendor MSTR]
        end

        subgraph Obs["Observability"]
            CW[CloudWatch Logs/Metrics]
        end

        ECR[ECR]
    end

    User -->|1 Load app| CF
    CF --> WAF --> S3S
    User -->|2 Login OIDC| Okta
    Okta -->|JWT| User
    User -->|3 API call + JWT| APIGW
    APIGW -->|VPC Link| ALB --> API

    API -->|4 enqueue long-running| SQS --> WRK
    API -->|5 intent parsing| BV
    API -->|chat, audit, embeddings| RDS
    API -->|session, job status| REDIS
    WRK -->|audit| RDS
    WRK -->|status| REDIS

    WRK -->|6 MSTR REST via PrivateLink| PL --> MSTR

    API -.reads.-> SM
    WRK -.reads.-> SM
    API -.logs.-> CW
    WRK -.logs.-> CW
    API -.traces.-> XR
    EKSBox -.pulls images.-> ECR
```

**Legend:** solid = request path · dashed = supporting services (secrets, logs)

---

## End-to-End Flow (one user action)

1. User opens app → CloudFront serves React from S3
2. User logs in via Okta → gets JWT with Azure AD group claims
3. User chats: "refresh cube ABC" → React sends request + JWT to API Gateway
4. API Gateway validates JWT with Okta JWKS → forwards to internal ALB → FastAPI pod
5. FastAPI checks AD groups (RBAC), calls BlueVerse Foundry to parse intent
6. Long-running op → FastAPI drops job on SQS, returns `{job_id}` immediately
7. Worker pod picks up job, calls MSTR REST via **PrivateLink endpoint** → vendor MSTR
8. Worker writes status to Redis (live) + Postgres (audit with real user ID)
9. React polls `/jobs/{id}` → reads Redis → updates UI

---

## AWS Services — Why Picked

| # | Service | Purpose | Why it's the best fit |
|---|---|---|---|
| 1 | **Route 53** | DNS | Native AWS, integrates with ACM + CloudFront |
| 2 | **CloudFront** | CDN for React SPA | Edge caching, DDoS shield, WAF attach point |
| 3 | **S3** | React static hosting + PDF/report artifacts | Cheapest object store, native CloudFront origin |
| 4 | **AWS WAF** | OWASP rules, rate limiting | AWS-native, attaches to CloudFront + API Gateway |
| 5 | **ACM** | Free TLS certs, auto-renew | No manual cert rotation |
| 6 | **API Gateway (HTTP API)** | Public entry + Okta JWT validation + throttling | Validates Okta tokens with zero app code |
| 7 | **Internal ALB** | Routes API GW → EKS | L7 routing, health checks, EKS ingress-friendly |
| 8 | **EKS** (EC2 node groups) | Container orchestration | Client mandate — Fargate not allowed in client AWS account |
| 9 | **ECR** | Private container registry | VPC-native, IAM-scoped, no Docker Hub limits |
| 10 | **SQS** | Decouple API from long MSTR polls | Managed, cheap, fits job-queue pattern |
| 11 | **RDS PostgreSQL** | Audit log, chat history, pgvector RAG | Standard Postgres, cheaper than Aurora |
| 12 | **ElastiCache Redis** | Session cache + live job status | Sub-ms reads; DB is not a cache |
| 13 | **Secrets Manager** | MSTR, DB, Okta creds | Auto-rotation, IAM-scoped per pod (IRSA) |
| 14 | **KMS** | Encryption keys at rest | Customer-managed keys for compliance |
| 15 | **VPC + NAT Gateway** | Network isolation + egress | Required for private workloads |
| 16 | **PrivateLink Endpoint** | Enterprise ↔ vendor MSTR | Already in place; no public MSTR exposure |
| 17 | **CloudWatch** | Logs, metrics, alarms | Native, cheap, zero setup for EKS/ALB/API GW |
| 19 | **IAM + IRSA** | Pod-level AWS permissions | Least privilege per pod |

---

## External (not AWS)

| System | Role |
|---|---|
| **Okta** | Identity provider — OIDC login, issues JWT |
| **Azure AD** | Group source, federated into Okta |
| **BlueVerse Foundry** | External LLM for NLP intent parsing |
| **MSTR** | Target system in vendor network |
