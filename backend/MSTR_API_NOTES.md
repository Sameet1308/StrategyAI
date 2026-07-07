============================== AREA: Authentication and sessions
[verified] auth_login: POST /auth/login
   headers: Content-Type: application/json (or application/x-www-form-urlencoded; mstrio-py sends form-encoded for standard login, JSON for API-token login)
   body: {"loginMode": 1, "username": "admin", "password": "pw", "applicationType": 76, "applicationId": "<optional custom app id>", "workingSet": 10, "maxSearch": 3, "metadataLocale": "en_us", "warehouseDataLocale": "en_us", "displayLocale": "en_us", "messagesLocale": "en_us", "numberLocale": "en_us", "timeZone": "UTC"} — loginMode values: 1=Standard, 8=Anonymous, 16=LDAP, 4096=API Token (verified in mstrio-py LoginMode enum). For loginMode 4096 the body is {"loginMode": 4096, "username": "<apiToken>", "applicationType": 76} (the API token goes in the username field, no password). All fields except loginMode+credentials are optional.
   resp: 204 No Content. Auth token returned in response header X-MSTR-AuthToken (NOT in body). Container also sets session cookie(s) (e.g. JSESSIONID) that MUST be persisted and replayed with the token on every subsequent call. 401 with body {"code": "ERR003", "message": ...} on bad credentials.
   ver: Current in Strategy One 2026 releases. applicationType 76 (DssXmlApplicationPython) and applicationId body field are recent additions (~11.4.x era); older servers only accept applicationType 35. loginMode 4096 (API Token) added around Update 10/11 era and is current. No v2 path variant — /api/auth/l
[verified] auth_logout: POST /auth/logout
   headers: X-MSTR-AuthToken; session cookies (JSESSIONID); X-MSTR-ProjectID must NOT be sent (mstrio-py explicitly nulls it for this call)
   resp: 204 No Content on success; 401 {"code": "ERR009"} if the token is already invalid/expired (mstrio-py whitelists ERR009/401 here).
   ver: Stable v1 path, unchanged.
[verified] session_status: GET /sessions
   headers: X-MSTR-AuthToken; session cookies; X-MSTR-ProjectID must NOT be sent (mstrio-py sets it to None for this call)
   resp: 200 with JSON containing at least {"timeout": <seconds>, "workingSet": n, "maxSearch": n, ...} (mstrio-py reads exactly these three keys); 401 if session expired.
   ver: Stable v1 path.
[verified] session_keep_alive: PUT /sessions
   headers: X-MSTR-AuthToken; session cookies; X-MSTR-ProjectID must NOT be sent
   resp: 204 No Content on success; non-2xx/401 when the session is gone (caller should re-login).
   ver: Stable v1 path. mstrio-py aliases Connection.renew = connect (renew-then-relogin pattern).
[verified] identity_token_create: POST /auth/identityToken
   headers: X-MSTR-AuthToken; session cookies
   resp: Success returns the new token in response header X-MSTR-IdentityToken (mstrio-py reads response.headers['X-MSTR-IdentityToken']).
   ver: Stable v1 path.
[verified] identity_token_validate: GET /auth/identityToken
   headers: X-MSTR-IdentityToken: <token>; X-MSTR-AuthToken (sent by mstrio-py session defaults)
   resp: 200 if the identity token is valid; 401 otherwise.
   ver: Stable v1 path.
[verified] auth_delegate: POST /auth/delegate
   headers: session cookies from the originating session; no pre-existing X-MSTR-AuthToken required
   body: {"loginMode": "-1", "identityToken": "<identity token>"} — loginMode is the literal string "-1" in mstrio-py
   resp: Success returns new X-MSTR-AuthToken in response header (plus cookies). 401 {"code": "ERR003"} if identity token invalid (mstrio-py whitelists ERR003/401 and falls back to credential login).
   ver: Stable v1 path.
[verified] api_token_create: POST /auth/apiTokens
   headers: X-MSTR-AuthToken; session cookies
   body: Optional JSON body {"userId": "<32-char user id>"} — when present (admin flow) creates the API token for that user; when omitted, creates it for the calling user.
   resp: 200/201 with JSON body {"apiToken": "<token>"} (mstrio-py reads response.json()['apiToken']).
   ver: Added ~Update 11 era (mstrio-py NEWS: 'added support for logging in with API Token', 'added get_api_token method... creating API Token by administrator for the user'). Current in 2026 releases; requires the API Token auth mode (4096) to be enabled on the environment.
[verified] session_user_privileges: GET /sessions/privileges
   headers: X-MSTR-AuthToken; session cookies
   resp: 200 with JSON privileges list (per-project validity included).
   ver: Stable v1 path.
[verified] session_user_info: GET /sessions/userInfo
   headers: X-MSTR-AuthToken; session cookies
   resp: 200 with JSON user info object.
   ver: Stable v1 path.
   NOTES: All paths above are relative to https://<server>/MicroStrategyLibrary/api (mstrio-py source hardcodes them as '/api/...'). Auth header pattern for EVERY subsequent call after login: (1) X-MSTR-AuthToken: <token from login response header>; (2) replay ALL cookies from the login response (JSESSIONID plus any load-balancer cookies) — the REST layer relies on the HTTP session, so token-without-cookie fails against clustered/Tomcat deployments; (3) X-MSTR-ProjectID: <32-char project id> for project-scoped APIs — mstrio-py sets it as a requests.Session default header after select_project, and explicitly nulls it for /auth/logout, GET /sessions, and PUT /sessions (sending a ProjectID on the sessions endpoints is wrong). There is NO GET /auth/token endpoint — session validation is GET /sessions (200 = alive, 401 = dead). Keep-alive pattern: PUT /sessions with a short timeout; on failure re-POST /auth/login and replace the token (mstrio-py's _renew_or_reconnect). Two independent timeouts exist: HTTP session (Tomcat web.xml) and I-Server session (must be < HTTP timeout); GET /sessions returns the effective 'timeout' in seconds. loginMode values verified in vendor enum: 1 Standard, 8 Anonymous, 16 LDAP, 4096 API Token; delegate flow uses loginMode "-1"; SAML/OIDC logins are browser-flow (not scriptable via /auth/login body) — for headless service accounts on an Okta-fronted deployment, the API Token mode (4096) is the intended pattern. Content type: official swagger uses JSON body for l
============================== AREA: Subscriptions and schedules (Strategy One / MicroStrategy REST API, March 2026 platform — spec title "Strategy REST 2026")
[verified] list_subscriptions: GET /subscriptions
   headers: X-MSTR-ProjectID (REQUIRED); X-MSTR-ClientVersion (optional; mstrio-py sends '25.06' to unlock newer response fields)
   query: offset (default 0), limit (default 1000; mstrio passes -1 for no limit), deliveryModes (int bitmask per EnumDSSXMLSubscriptionDeliveryType, default -1 = all), lastRun (bool, default false; adds lastRun timestamp per subscription; requires I-Server >= 11.4.0600), fields (comma-separated top-level whitelist)
   resp: 200 OK. {"subscriptions": [{id, name, multipleContents, editable (bool, read-only), allowDeliveryChanges, allowPersonalizationChanges, allowUnsubscribe, allowUnsubscribeFromEmail, dateCreated, dateModified, owner:{id,name}, schedules:[{id, name, type: event_based|time_based|unknown, nextDelivery, expired}], contents:[{id, name, type: report|document|dossier, personalization:{compressed, formatMode, formatType, viewMode}}], recipients:[{id, name, isGroup, type: user|user_group|contact|contact_gro
   ver: lastRun query param only honored on 11.4.0600+ (mstrio gates it). deliveryModes filter and softDisabled field are present in the 2026 spec.
[verified] get_subscription: GET /subscriptions/{id}
   headers: X-MSTR-ProjectID (spec marks optional, but docs and mstrio always send it — treat as required for project-scoped subscriptions); X-MSTR-ClientVersion (optional)
   query: fields
   resp: 200 OK. Full Subscription object — same shape as one element of the list response (id, name, editable, owner, schedules[], contents[], recipients[] with childSubscriptionId, delivery{mode, softDisabled, email{...}}, dateCreated/Modified, lastRun, nextDelivery, alert). Current enable/disable state is read from delivery.softDisabled.
[verified] create_subscription: POST /subscriptions
   headers: X-MSTR-ProjectID (required in practice; docs sample sends it)
   query: fields
   body: {"name": "...", "allowDeliveryChanges": false, "allowPersonalizationChanges": false, "allowUnsubscribe": false, "sendNow": false, "schedules": [{"id": "<scheduleId>"}], "contents": [{"id": "<objId>", "type": "report|document", "personalization": {"compressed": false, "formatMode": "CURRENT_PAGE", "viewMode": "BOTH", "formatType": "HTML|PDF|EXCEL|CSV"}}], "recipients": [{"id": "<userId>", "type": "user", "includeType": "TO"}], "delivery": {"mode": "EMAIL", "contactSecurity": false, "email": {"subject": "...", "filename": "...", "sendContentAs": "data", "overwriteOlderVersion": false}}}. Required: name, delivery. sendNow:true executes immediately on create.
   resp: 201 Created with the full created Subscription (including generated id and recipients[].childSubscriptionId).
[verified] update_subscription: PUT /subscriptions/{id}
   headers: X-MSTR-ProjectID (mstrio always sends; spec optional); X-MSTR-ClientVersion (optional)
   query: fields
   body: Full Subscription object (whole-object replace, NOT a diff): {"name", "allowDeliveryChanges", "allowPersonalizationChanges", "allowUnsubscribe", "multipleContents", "sendNow" (bool, write-only — true also triggers immediate send), "owner": {"id"}, "schedules": [{"id"}], "contents": [...], "recipients": [...], "delivery": {...}}. mstrio builds the body from current object state and strips nulls; for prompted content it must include personalization.prompt {enabled:true, instanceId} from a re-prompted instance.
   resp: 200 OK with the updated Subscription JSON.
[verified] patch_subscription_enable_disable: PATCH /subscriptions/{id}
   headers: X-MSTR-ProjectID (spec optional; send it)
   body: SubscriptionPatch — all fields optional: {"softDisabled": true|false, "name": "..."}. softDisabled:true = pause (subscription stays defined but is not delivered); false = resume.
   resp: 204 No Content on success.
   ver: Present in the Strategy REST 2026 spec (soft-disable is a recent Strategy One feature). NOT wrapped by mstrio-py as of master 2026-06-18 (mstrio only exposes PUT). Verify against the client's Library server api-docs before relying on it in older environments.
[verified] delete_subscription: DELETE /subscriptions/{id}
   headers: X-MSTR-ProjectID (spec lists only X-MSTR-AuthToken, but both official docs sample and mstrio send X-MSTR-ProjectID — send it)
   resp: 204 No Content, empty body.
[verified] send_subscription_now_v2: POST /v2/subscriptions/{id}/send
   headers: X-MSTR-ProjectID
   query: fields
   body: Optional; only needed to override prompt answers: {"contents": [{"contentId": "<objId>", "instanceId": "<instanceId with answered prompts>"}]} (schema PromptInfos). Empty/no body sends with stored answers.
   resp: 202 Accepted, empty body (async — delivery runs server-side; poll GET /subscriptions/{id}/status for progress).
   ver: v2 accepts multiple contents' prompt answers (PromptInfos array); v1 accepts a single PromptInfo. Neither is flagged deprecated in the 2026 spec, but mstrio uses v2 — prefer v2.
[verified] send_subscription_now_v1: POST /subscriptions/{id}/send
   headers: X-MSTR-ProjectID
   query: fields
   body: Optional single-content prompt override: {"contentId": "...", "instanceId": "..."} (schema PromptInfo).
   resp: 202 Accepted, empty body.
   ver: Superseded in practice by /v2 for multi-content subscriptions.
[verified] get_subscription_status: GET /subscriptions/{id}/status
   headers: X-MSTR-ProjectID (optional per spec; mstrio omits it here)
   query: fields
   resp: 200 OK. {id, stage (int), state (int), total, failure, estimate (seconds remaining), expiration, start, end, contents: [{id, name, type, ...}], statuses: [{...per-recipient/delivery detail, errors}]}
   ver: mstrio gates status/lastRun behind I-Server 11.4.0600+; also raises NotSupportedError for delivery modes SNAPSHOT/PERSONAL_VIEW/SHARED_LINK/UNSUPPORTED.
[verified] list_subscriptions_cross_projects: POST /subscriptions/query
   query: deliveryModes (default -1), lastRun (bool), fields
   body: {"projectIds": ["<projectId1>", "<projectId2>"]}
   resp: 200 OK. Same {"subscriptions": [...]} shape as GET /subscriptions but across the given projects. Optional header mstr-http-method-override.
   ver: 2026-spec endpoint; not wrapped by mstrio-py.
[verified] list_schedules_v2: GET /v2/schedules
   query: offset (default 0), limit (default 500), includeHidden (default false), fields
   resp: 200 OK. {"schedules": [{name, id, description, scheduleType: time_based|event_based|unknown, scheduleNextDelivery, startDate (yyyy-MM-dd), stopDate, time: {recurrencePattern: daily|weekly|monthly|yearly, execution: {executionPattern: once|repeat, startTime, stopTime, repeatInterval}, daily/weekly/monthly/yearly sub-objects}, event: {eventId, name} (NOTE: field is 'eventId', not 'id'), expired (bool), acg, timezone, hidden, dateCreated, dateModified, owner}]}
   ver: v1 GET /schedules still exists (no paging, Schedule schema without hidden/owner/dates). Use v2 for lists.
[verified] list_schedules_v1: GET /schedules
   query: fields
   resp: 200 OK. {"schedules": [{name, id, description, scheduleType, scheduleNextDelivery, startDate, stopDate, time{...}/event{eventId,...}, expired, acg, timezone}]} — no offset/limit, no hidden/owner metadata.
[verified] get_schedule: GET /schedules/{id}
   query: fields
   resp: 200 OK. Single ScheduleV2 object: {name, id, description, scheduleType, scheduleNextDelivery, startDate, stopDate, time{...} (time_based) or event{eventId, name} (event_based), expired, acg, timezone, hidden, dateCreated, dateModified, owner{id,name}, tenant}.
   ver: Event-based schedules use 'eventId' (not 'id') inside the event object in both request and response bodies — mstrio explicitly renames it, calling it an 'API Problem'.
[verified] update_schedule: PUT /schedules/{id}
   query: fields
   body: Schedule object: {"name", "description", "scheduleType": "time_based|event_based", "startDate": "yyyy-MM-dd", "stopDate": "yyyy-MM-dd", "time": {...} or "event": {"eventId": "..."}, "timezone"} plus optional {"changeJournal": {"userComments": "..."}} (11.5.1200+).
   resp: 200 OK with updated ScheduleV2.
   ver: changeJournal.userComments in PUT body requires 11.5.1200+ (mstrio gates it).
[verified] delete_schedule: DELETE /schedules/{id}
   query: userComments (optional change-journal comment)
   resp: 204 No Content.
   NOTES: All paths are relative to /MicroStrategyLibrary/api; X-MSTR-AuthToken header is required on every call (obtained from POST /auth/login, returned in the X-MSTR-AuthToken response header alongside a JSESSIONID cookie — send both back). Ground truth: live OpenAPI 3.0.1 spec titled "Strategy REST 2026" fetched from https://demo.microstrategy.com/MicroStrategyLibrary/api/openapi.json (the client's own server exposes the same at /MicroStrategyLibrary/api/openapi.json — use it to confirm PATCH softDisabled exists on their build), cross-checked with mstrio-py master commit 88c3ca79 (2026-06-18). GOTCHAS: (1) Enable/disable = "soft disable": PATCH /subscriptions/{id} {"softDisabled": bool} returns 204; current state is read from delivery.softDisabled in GET responses; there is no enable/disable field in the PUT top level and mstrio-py does not wrap PATCH yet. (2) Send-now is async: POST /v2/subscriptions/{id}/send returns 202 with EMPTY body (no instance id); poll GET /subscriptions/{id}/status (stage/state/total/failure/estimate/statuses) to track delivery — status endpoint requires 11.4.0600+ and is unsupported for SNAPSHOT/PERSONAL_VIEW/SHARED_LINK delivery modes. (3) PUT /subscriptions/{id} is whole-object replace — GET first, mutate, PUT back; omitting schedules/recipients/delivery drops them; body also supports write-only sendNow:true. For prompted content the PUT body must carry personalization.prompt {enabled:true, instanceId} from a freshly answered instance (mstrio re-prompt
============================== AREA: Intelligent cubes — status, publish/refresh, caches (Strategy One REST API, March 2026 release)
[verified] get_cube_info: GET /cubes/?id={cubeId}
   headers: X-MSTR-ProjectID
   query: id (cube GUID, required)
   resp: 200. {"cubesInfos": [{"cubeName", "cubeId", "size", "status" (integer bitfield, see notes), "path", last-modified date, owner name/ID, server mode}]}. mstrio reads .json()['cubesInfos'][0] and uses it to populate size, status, path, ownerId, serverMode.
   ver: v1 path; still the current way to get cube size/status/path metadata. mstrio attaches X-MSTR-ProjectID as a session-wide header (connection.py line 677), so a project must be selected.
[verified] get_cube_status_head: GET /cubes/{cubeId}  (HTTP method is HEAD, not GET — schema enum forces GET here; use HEAD)
   headers: X-MSTR-ProjectID
   resp: 200 with NO body. Status returned in response header X-MSTR-CubeStatus as an integer bit vector from EnumDSSCubeStates. mstrio: int(res.headers['X-MSTR-CubeStatus']).
   ver: IMPORTANT: actual HTTP method is HEAD. v1 path, still current — this is what mstrio's Cube.refresh_status() polls after publish/refresh.
[verified] publish_cube: POST /v2/cubes/{cubeId}
   headers: X-MSTR-ProjectID
   body: empty body
   resp: 202 Accepted. {"id": "478:RU5WLTI4MjA0MExBSU9VU0Ux" (jobId:base64-node), "jobId": 478}
   ver: Available since 2021 Update 5; current in March 2026 release. There is NO separate /cubes/{id}/refresh endpoint — republish IS refresh. Async: 202 + jobId, poll via GET /v2/monitors/jobs/{jobId}.
[verified] get_job_status_v2: GET /v2/monitors/jobs/{jobId}
   query: fields (optional comma-separated whitelist)
   resp: 200. Job object (id, status, type, user, project, duration etc.). 404/400 once job completes and is purged — treat 'job gone' as finished.
   ver: Use v2 (/v2/monitors/jobs). v1 GET /monitors/jobs (list, params incl. clusterNode) exists but mstrio prefers v2 when server supports it.
[verified] list_iserver_nodes: GET /monitors/iServer/nodes
   query: projects.id (optional project GUID filter), name (optional node name filter)
   resp: 200. {"nodes": [{"name": <node name>, address/port, runtime status, "projects": [...]}]} — mstrio reads nodes[].name to feed clusterNode params elsewhere.
   ver: v1, current.
[verified] list_cube_caches: GET /monitors/caches/cubes
   query: clusterNode (REQUIRED — node name from GET /monitors/iServer/nodes; wrong name => 503), projectIds (comma-separated GUIDs), state.loadedState=loaded (only filter value supported), sortBy (e.g. +name, -size; fields: name,size,hitCount,lastUpdateTime,status,project,owner), offset (default 0), limit (default 1000, must be 1..1000; mstrio passes -1 for 'no limit')
   resp: 200. {"offset", "limit", "total", "loaded", "unloaded", "cubeCaches": [{"id" (format cubeCacheGuid:projectId:base64(node)), "projectId", "source": {"id","type":"cube","name"}, "state": {"active","dirty","infoDirty","persisted","processing","loadedState": loaded|unloaded|loadPending|unloadPending}, "lastUpdateTime", "hitCount", "size" (bytes), "creatorName", "creatorId", "lastUpdateJob", "openViewCount", "creationTime", "historicHitCount", "databaseConnection", "fileName", "rowCount", "columnCoun
   ver: v1, current. Available since 2021.
[verified] get_cube_cache_info: GET /monitors/caches/cubes/{cacheId}
   resp: 200. Single cache object, same shape as one element of cubeCaches[] above (state, size, rowCount, columnCount, fileName, jobExecutionStatistics, etc.). 400 invalid cacheId, 404 not found.
   ver: v1, current.
[verified] alter_cube_cache_status: PATCH /monitors/caches/cubes/{cacheId}
   headers: Prefer: respond-async (REQUIRED per official docs; mstrio always sends it)
   body: {"state": {"active": true|false}}  OR  {"state": {"loadedState": "loaded"|"unloaded"}} — exactly ONE of active/loadedState per request, never both
   resp: 202 Accepted. {"manipulationId": "3D9062...A297:MTAuMjMuNi4yMDY=", "status": "executing"}. Location response header contains the polling URI.
   ver: v1, current. Async 202 pattern with manipulation-status polling (not job polling).
[verified] get_cube_cache_manipulation_status: GET /monitors/caches/cubes/manipulations/{manipulationId}/status
   resp: 200. {"status": "ready"|"executing"|"error"|"requestHandlingFailed", "statusDetail": "..."}. ready = done OK; executing = still processing; error = finished with error; requestHandlingFailed = I-Server rejected the request. 400 invalid manipulation id.
   ver: v1, current.
[verified] delete_cube_cache: DELETE /monitors/caches/cubes/{cacheId}
   resp: 204 No Content on success (synchronous — mstrio checks status_code == 204). 400 invalid cacheId, 404 not found.
   ver: v1, current. Delete is always allowed regardless of cache state.
[verified] get_cube_cache_aggregated_usage: GET /monitors/caches/cubes/aggregatedUsages
   query: clusterNode (required), groupByObject (user | project only)
   resp: 200. Aggregated cache capacity usage grouped by project or user. 400 for invalid clusterNode or groupByObject.
   ver: v1, current.
[verified] get_cube_definition_v2: GET /v2/cubes/{cubeId}
   headers: X-MSTR-ProjectID
   resp: 200. Cube definition: available attributes and metrics (no data query executed).
   ver: v2 is current for all cube execution/definition endpoints.
[verified] create_cube_instance: POST /v2/cubes/{cubeId}/instances
   headers: X-MSTR-ProjectID
   query: offset (default 0), limit (default 5000 in mstrio), fields=-data.metricValues.extras,-data.metricValues.formatted (perf optimization, servers >= 11.2.0200)
   body: {} or view filter / requestedObjects body
   resp: 200/201. Instance with "instanceId" plus first chunk of data. Poll/pagination via companion GET.
   ver: v2 current; v1 /cubes/{id}/instances deprecated.
[verified] publish_super_cube_upload_session: POST /datasets/{datasetId}/uploadSessions/{sessionId}/publish
   headers: X-MSTR-ProjectID
   body: empty body
   resp: 202-style async; poll GET /datasets/{datasetId}/uploadSessions/{sessionId}/publishStatus for progress; DELETE /datasets/{datasetId}/uploadSessions/{sessionId} cancels publication.
   ver: v1 datasets API; still the current mechanism for push-data super cubes.
   NOTES: Key gotchas, all verified against mstrio-py master (vendor SDK) and official docs pages last updated Jun 18 2026 (current for March 2026 'Strategy One' release):

1) X-MSTR-ProjectID handling: mstrio sets X-MSTR-AuthToken AND X-MSTR-ProjectID as DEFAULT session headers on every request once a project is selected (connection.py lines 663-677, 1116). So all /cubes and /v2/cubes calls implicitly require X-MSTR-ProjectID. The monitors endpoints (/monitors/caches/cubes*, /monitors/iServer/nodes) do NOT need it — get_node_info explicitly overrides it to None. Cache operations are addressed by composite cacheId + clusterNode, not by project header.

2) Refresh == republish: there is no /cubes/{id}/refresh endpoint. POST /v2/cubes/{cubeId} with empty body both publishes and refreshes; 202 + {id: 'jobId:base64node', jobId: int}. Poll GET /v2/monitors/jobs/{jobId}, and/or HEAD /cubes/{cubeId} reading the X-MSTR-CubeStatus header.

3) Cube status bitfield (X-MSTR-CubeStatus header, EnumDSSCubeStates): PROCESSING=1, ACTIVE=2, PERSISTED=4, DIRTY_INFO=8, DIRTY=16, LOADED=32, READY=64, LOAD_PENDING=128, UNLOAD_PENDING=256, PENDING_FOR_ENGINE=512, IMPORTED=1024, FOREIGN=2048. Same integer appears as 'status' in GET /cubes/?id= response (cubesInfos[0].status). Decode greedily from the largest bit down.

4) Two DIFFERENT async patterns: (a) cube publish -> 202 + jobId -> poll /v2/monitors/jobs/{id}; (b) cache manipulation (PATCH cache) -> requires 'Prefer: respond-async' header -> 202 + manipu
============================== AREA: Projects list and object search (name → ID resolution): GET /projects, quick search GET /searches/results, full metadata search (v1/v2), object details GET /objects/{id}
[verified] list_projects: GET /projects
   resp: 200 OK. JSON array: [{"id": "<32-hex>", "name": "MicroStrategy Tutorial", "alias": "", "description": "...", "status": 0}]. status 0 = loaded/active.
   ver: v1 path, unchanged and current in mstrio-py master (supports Strategy One 2026 releases).
[verified] get_project_by_name: GET /projects/{name}
   resp: 200 OK. Single project object: {id, name, alias, description, status}.
   ver: v1, current.
[verified] quick_search_objects: GET /searches/results
   headers: X-MSTR-ProjectID (scopes search to one project; official workflow always passes it; omit + isCrossCluster=true to search across cluster projects)
   query: name (search string), pattern (int, EnumDSSXMLSearchTypes: 0=CONTAINS_ANY_WORD, 1=BEGIN_WITH, 2=EXACTLY, 3=BEGIN_WITH_PHRASE, 4=CONTAINS (default), 5=END_WITH), type (repeatable; ObjectTypes or ObjectSubTypes int codes, e.g. type=3 for reports+cubes, type=776 / type=779 for cube subtypes), root (folder ID to search under), offset (default 0), limit (-1 = no limit), getAncestors (bool), certifiedStatus (ALL | CERTIFIED_ONLY | NOT_CERTIFIED_ONLY | OFF), result.hidden (bool), isCrossCluster (bool), tenantIds (list)
   resp: 200 OK. {"totalItems": N, "result": [{"id", "name", "type": 3, "subtype": 768|776|779..., "extType", "description", "dateCreated", "dateModified", "version", "acg", "viewMedia", "owner": {"id", "name"}, "certifiedInfo": {"certified": bool}, "ancestors" (if getAncestors=true)}]}. Response header x-mstr-total-count mirrors totalItems. Default/null fields are omitted per object.
   ver: v1 path, current in mstrio-py master. tenantIds/isCrossCluster/result.hidden are newer additions present in current SDK.
[verified] quick_search_by_search_object: GET /searchObjects/{search_object_id}/results
   headers: X-MSTR-ProjectID (required — mstrio signature makes project_id mandatory)
   query: includeAncestors (bool), includeAcl (bool), result.subtypes (repeatable subtype ints, e.g. 776, 779), offset, limit
   resp: 200 OK. Same result-list shape as /searches/results ({totalItems, result: [...]}).
   ver: Available since 11.3.0100 per mstrio @method_version_handler; current.
[probable] quick_search_post_body: POST /searches/objects
   query: includeAncestors (bool), showNavigationPath (bool), fields (comma-separated field whitelist)
   body: { ...search criteria dict... } — mstrio passes an opaque criteria body (name/pattern/types/root etc. in JSON form); exact schema not shown in SDK source
   resp: 200 OK, result-list shape.
   ver: Newer addition present in current mstrio-py master.
[verified] full_metadata_search_create_instance_v1: POST /metadataSearches/results
   headers: X-MSTR-ProjectID (project scope; omit for configuration-level search)
   query: name, pattern (int, default 4=CONTAINS), domain (int: 1=LOCAL, 2=PROJECT (default), 3=REPOSITORY, 4=CONFIGURATION), root (folder ID), type (repeatable type/subtype ints), usesObject ('objectId;type', e.g. 'E02FE6DC...;3'), usesRecursive, usesOneOf, usedByObject, usedByRecursive, usedByOneOf, beginModificationTime (yyyy-MM-dd'T'HH:mm:ssZ), endModificationTime
   resp: 200 OK. {"id": "<searchId>", "totalItems": N} — id feeds GET /metadataSearches/results?searchId=.
   ver: v1 available since 11.3.0000; still valid but superseded by v2 for servers >= 11.4.1200 (mstrio auto-selects v2 when server supports it).
[verified] full_metadata_search_create_instance_v2: POST /v2/metadataSearches/results
   headers: X-MSTR-ProjectID (project scope)
   query: name, pattern, domain, scope (managed-objects scope: 'rooted' | 'not_managed_only' | 'managed_only' | 'all'), root, type (repeatable), usesObject, usesRecursive, usesOneOf, usedByObject, usedByRecursive, usedByOneOf, beginModificationTime, endModificationTime, visibility ('visible' | 'all' — filters on hidden flag)
   body: {"includeSubfolders": bool, "dateFilterType": str, "timeRange": {...}, "descriptionQuery": str, "ownerId": str, "localeId": int, "excludedFolders": ["folderId"]} (camelCase; None fields stripped)
   resp: 200 OK. {"id": "<searchId>", "totalItems": N}.
   ver: Requires I-Server 11.4.1200+; this is the current variant on the March 2026 platform release. Results are still fetched via the v1 GET endpoints below.
[verified] full_metadata_search_get_results: GET /metadataSearches/results
   headers: X-MSTR-ProjectID (same scope used when instance was created)
   query: searchId (required, from the POST), offset (default 0), limit (-1 = no limit; use batches for large result sets — Java heap errors possible otherwise)
   resp: 200 OK. Object list; per-object fields: id, name, description, type, subtype, dateCreated, dateModified, acg, owner.
   ver: v1, current (used for both v1 and v2 search instances). Legacy alternative GET /objects?searchId= also still exists (mstrio uses it in its async helper).
[verified] full_metadata_search_get_results_tree: GET /metadataSearches/results/tree
   headers: X-MSTR-ProjectID
   query: searchId (required), offset, limit
   resp: 200 OK, nested tree dict.
   ver: v1, current.
[verified] get_object_info: GET /objects/{id}
   headers: X-MSTR-ProjectID (project the object lives in; if omitted, resolves against the non-project/configuration area only)
   query: type (required, int EnumDSSXMLObjectTypes — e.g. 3 report/cube, 8 folder, 55 document/dashboard, 34 user, 44 security role, 32 project), comments (bool; mstrio sends comments=true to include long description/comments)
   resp: 200 OK. {id, name, type, subtype, extType, description, dateCreated, dateModified, version, acg, owner{id,name}, ancestors, comments, ...}.
   ver: v1, current.
[probable] get_search_suggestions: GET /searches/suggestions
   headers: X-MSTR-ProjectID (optional project scope)
   query: key (search string), count (max items, -1 = no limit), isCrossCluster (bool)
   resp: 200 OK, list of suggestion strings/objects.
   ver: Present in current mstrio-py master.
   NOTES: TYPE/SUBTYPE CODES (verified in mstrio/types.py, mirrors EnumDSSXMLObjectTypes/SubTypes): object type 3 = REPORT_DEFINITION and covers BOTH reports and intelligent cubes; cubes are distinguished by SUBTYPE. Subtype = type*256 + n. Key subtypes under type 3: 768 REPORT_GRID, 769 REPORT_GRAPH, 774 REPORT_GRID_AND_GRAPH, 776 OLAP_CUBE (classic Intelligent Cube; REST string alias 'report_cube'), 777 INCREMENTAL_REFRESH_REPORT, 779 SUPER_CUBE (EMMA/data-import 'super cube'; REST alias 'report_emma_cube'), 780 SUPER_CUBE_IRR. Other useful types: 8 FOLDER, 55 DOCUMENT_DEFINITION (dashboards; subtype 14081 report-writing doc), 39 SEARCH (saved search objects), 12 ATTRIBUTE, 4 METRIC, 34 USER/USERGROUP, 32 PROJECT. To find all cubes pass type=776&type=779 (the 'type' query param accepts subtype codes and repeats per value — Python requests serializes lists as repeated params). SEARCH FLAVORS: (1) Quick search GET /searches/results — synchronous 200, index-based (may lag recent changes by the index refresh interval), X-MSTR-ProjectID header scopes to a project; without it + isCrossCluster=true searches all projects. (2) Full metadata search — two-step stored-instance pattern, NOT a 202/Location pattern: POST /metadataSearches/results (or /v2/... on 11.4.1200+, which the March 2026 release satisfies) returns {id, totalItems}; then page GET /metadataSearches/results?searchId=...&offset=&limit=. Live and supports uses/usedBy dependency queries; large result sets must be paged or I-Server 
============================== AREA: Platform version + REST API changes in recent releases (2025 series through the March 2026 Strategy One release)
[verified] whats_new_rest_api_changelog: GET (documentation page, not an API) https://microstrategy.github.io/rest-api-docs/whats-new/
   resp: n/a (docs page)
   ver: Release naming verified: current series is 'Strategy One (Month Year)'. On-prem platform releases are quarterly (Mar/Jun/Sep/Dec); monthly releases are cloud-first. The producthelp readme states the March 2026 on-prem release bundles the January 2026 and February 2026 cloud updates. Rebrand history:
[verified] create_api_token: POST /auth/apiTokens
   body: {"userId": "<optional target user id>"}
   resp: 200 with apiToken value in body
   ver: Auth change relevant to 2025: mstrio-py added API-token login (loginMode 4096) in 11.5.4 (Apr 2025) and admin-issued tokens for other users via User.get_api_token in 11.5.6 (Jun 2025). mstrio-py LoginMode enum: Standard=1, LDAP=16, API_TOKEN=4096. Also since Jun 2025 mstrio-py sessions identify as a
[probable] oauth2_token_endpoint: POST (Library root, NOT under /api) /MicroStrategyLibrary/oauth2/token — with /MicroStrategyLibrary/oauth2/authorize for the authorization step
   resp: OAuth2 token response (access_token, refresh_token)
   ver: Documented on the current (2026) docs site; exact release that introduced it not stated on the page (docs page last updated Jun 2026). Endpoint names verified from docs text; HTTP method for token endpoint is standard OAuth2 POST (not explicitly shown). Legacy POST /api/auth/login with X-MSTR-AuthTo
[probable] bot_apis_deprecated_renamed_to_agent: GET /bots (representative; family-level change)
   headers: X-MSTR-ProjectID
   resp: n/a (deprecation notice)
   ver: The deprecation itself is verified in two official sources; the exact list of removed REST paths is not published on the changelog page — do not build new integrations on /api/bots-style endpoints; use Agent APIs instead. On a March 2026 server the Bot APIs may already be unavailable.
[verified] retrieve_data_model: GET /model/dataModels/{dataModelId}
   headers: X-MSTR-ProjectID
   resp: 200 with data model definition JSON
   ver: New API family, rolled out Aug 2025 → May 2026. All of it is available on a March 2026 server except: Export/Restore Data Model + Create unstructured data (April 2026) and Save-as-new-data-model (May 2026). X-MSTR-ProjectID requirement inferred from sibling pages (cube-instance sample shows it expli
[verified] create_cube_instance_for_data_model: POST /v2/cubes/{dataModelId}/instances
   headers: X-MSTR-ProjectID
   body: optional; empty body accepted
   resp: 200 {id, name, instanceId, status:1, definition:{grid:{crossTab, rows[], columns[]}}, data:{...}}
   ver: Added December 2025 — available on March 2026 servers. Reuses the existing /api/v2/cubes execution surface (which itself is long-standing); the change is that a data model ID is now accepted as the cube ID. Companion Dec 2025 addition: Retrieve Data Model SQL View.
[verified] get_unstructured_data_categories: GET /nuggets/{id}/categories
   headers: X-MSTR-ProjectID
   resp: 200 {"<categoryName>": ["tag1", "tag2"], ...}
   ver: NEW in the client's exact release (March 2026). The family index page states these were 'previously available as internal APIs, now publicly accessible'.
[verified] create_unstructured_data: POST /nuggets?type=unstructuredData
   headers: X-MSTR-ProjectID
   query: type=unstructuredData (required)
   body: multipart/form-data: fileName (string), fileType (int), fileSize (int, optional), folderId (string), file (binary)
   resp: 200 {"id": "<unstructured data id>"}
   ver: April 2026 — NOT available on the client's March 2026 release (get/update category endpoints are; create is one month later). Verify server build before relying on it.
[verified] create_project_duplication: POST /projectDuplications
   body: {source:{environment:{id,...}, project:{id,...}}, target:{environment:{id,...}, project:{name,...}}, settings/DuplicationConfig...}
   resp: 202-style async: returns duplication id; poll GET /projectDuplications/{id} for status
   ver: Full verified family: POST /api/projectDuplications, GET /api/projectDuplications (list, offset/limit), GET /api/projectDuplications/{id}, PUT /api/projectDuplications/{id} , PUT /api/projectDuplications/{id}/status (cancel), GET /api/projectDuplications/versions, GET /api/projectDuplications/valida
[verified] change_journal_search: POST /changeJournal
   body: search criteria (projects, object ids, time range, filters)
   resp: POST returns searchId; GET returns change journal entries list
   ver: Added Nov 2025 — available on March 2026 servers. Useful for admin auditing (who changed what). Two-step async-ish pattern: create search then fetch by searchId.
[verified] run_python_script_evaluation: POST /scripts/{id}/evaluation
   headers: X-MSTR-ProjectID
   resp: evaluation id; poll evaluation endpoint for status/output
   ver: Feb 2026 (cloud) — included in the March 2026 on-prem release per the readme's bundling statement. X-MSTR-ProjectID header explicitly set on the delete call in mstrio-py source; assume project context required throughout. Async pattern: POST evaluation → poll GET /api/scripts/evaluation/{id}.
[verified] multitenant_tenant_management: GET /multitenant/tenant
   resp: tenant objects {id, name, members, status, suffix}
   ver: Paths verified from mstrio-py source; per-path HTTP methods not individually confirmed. Preview in March 2026, maturing through May/June 2026 (Platform Analytics multi-tenant support added March 2026).
[verified] list_history_list_messages_v2: GET /v2/historyList
   headers: X-MSTR-ProjectID
   resp: list of history list messages
   ver: Paths verified in vendor SDK; flagged preview as of May 2026 — endpoint may predate mstrio support, but treat behavior as not fully stable on March 2026 servers. Note the v2 path for listing vs v1-style path for deleteMessages.
[verified] get_jobs_v2_monitor: GET /v2/monitors/jobs
   query: nodeName, user, status, jobType, sortBy, fields
   resp: list of job monitor entries
   ver: Not a 2025-2026 change, but the v1/v2 state that holds in March 2026: use v2 job/cache-content monitor endpoints; no monitors endpoints were removed in 2025-2026. Other monitors (userConnections, dbConnectionInstances, caches/cubes, iServer/nodes, projects/status) remain v1-only.
[verified] send_subscription_v2: POST /v2/subscriptions/{subscriptionId}/send
   headers: X-MSTR-ProjectID
   resp: 202/204 on trigger
   ver: No subscription endpoint removals/renames in 2025-2026. Functional additions land as new enum values/payload fields, not new paths: delivery modes ONEDRIVE, SHAREPOINT, S3 recognized by Nov 2025 (mstrio 11.5.11); GCS (Google Cloud Storage) device/transmitter type May 2026 (11.6.5); key-based SFTP au
[verified] create_project_v2: POST /v2/projects
   body: {name, description, ...}
   resp: project creation accepted; poll project status
   ver: Also verified in projects.py: v2 project settings endpoints GET/PUT/PATCH /api/v2/projects/{id}/settings and GET /api/v2/projects/{id}/settings/config coexist with v1 engine settings /api/projects/{id}/settings/engine. setPlatformAnalytics is 2026-era (tenant PA arrived with multitenancy) — treat as
[probable] system_hierarchy_and_scope_filters: GET /model/systemHierarchy (representative; see docs workflow pages)
   headers: X-MSTR-ProjectID
   resp: hierarchy relationships / scope filter definitions
   ver: Change verified in official changelog; exact endpoint path not fetched (docs pages exist — fetch manage-system-hierarchy/retrieve-all-the-relationships-in-the-system-hierarchy for the literal path). Modeling-service endpoints follow the /api/model/* + changeset pattern and require X-MSTR-ProjectID.
[uncertain] project_documentation_api: POST /documentations (representative; definitions + executions family)
   resp: documentation job with status; export to file
   ver: SDK-verified feature but REST paths not confirmed (mstrio/api/documentation.py + documentation_definitions.py exist in repo). June 2026 — beyond the client's March 2026 release; listed only to mark the direction of the API surface.
   NOTES: VERSIONING STORY (verified across microstrategy.github.io/rest-api-docs/whats-new/, www2.strategy.com producthelp readme, and mstrio-py NEWS.md): (1) Product is now "Strategy One"; releases are named "Strategy One (Month Year)". The client's platform is "Strategy One (March 2026)" — a quarterly on-prem platform release that bundles the Jan 2026 + Feb 2026 cloud-only updates. 2024 releases were "MicroStrategy ONE (Month Year)"; the "Update N" naming ended at 2021 Update 11. (2) Version numbers: mstrio-py mirrors the platform — 11.5.x = 2025 series, 11.6.x = 2026 series; March 2026 <-> mstrio-py 11.6.3.101; internal I-Server version strings use the 11.x.xxxx form (e.g. feature gates at '11.3.0200', '11.3.1200' in vendor SDK source), so March 2026 ~ 11.6.0300-style build. (3) Official REST changelog lives at https://microstrategy.github.io/rest-api-docs/whats-new/ (docs site offers version pickers for 2024/2020 snapshots); product what's-new at https://www2.strategy.com/producthelp/current/readme/en-us/content/whats_new.htm. GOTCHAS FOR THE PROJECT'S TARGET AREAS: subscriptions, cubes(admin), monitors and auth endpoints saw NO removals or renames in 2025-2026 — the 2025-2026 additions are almost entirely new families (Mosaic Data Model /api/model/dataModels, Agent APIs replacing deprecated Bot APIs (Sep 2025), Unstructured Data /api/nuggets (Mar 2026), projectDuplications, changeJournal, scripts, multitenant, v2 historyList). Base URL and header contract unchanged: /MicroStrateg
============================== AUDIT GAPS
- pause_subscription: The designated mechanism (PATCH /subscriptions/{id} with body {"softDisabled": true}, 204 No Content) is verified from a single source only — the live demo-server OpenAPI spec ('Strategy REST 2026'). It is not wrapped by mstrio-py, has no official do
- resume_subscription: Same single-source risk as pause_subscription: PATCH /subscriptions/{id} {"softDisabled": false} is verified only against the demo-server OpenAPI spec, not wrapped by mstrio-py, and flagged by the research itself as needing confirmation on the client
- get_refresh_status: The polling endpoint GET /v2/monitors/jobs/{jobId} is verified, but critical detail for a poller is missing: the response shape is only sketched ('Job object (id, status, type, user, project, duration etc.)') with no enumeration of the 'status' field
============================== GAPFILL AREA: gap:pause_subscription
[verified] pause_subscription_patch: PATCH /subscriptions/{id}
   headers: X-MSTR-ProjectID (spec marks it required=false, but every other /subscriptions/{id} op is project-scoped and mstrio-py always sends it on GET/PUT/DELETE — always send it)
   body: {"softDisabled": true}  — schema SubscriptionPatch: {name?: string(1..250), softDisabled?: boolean (writeOnly)}; all fields optional, requestBody itself required. NOTE: softDisabled is TOP-LEVEL in the PATCH body, unlike GET/PUT where it lives at delivery.softDisabled
   resp: 204 No Content, empty body. Errors 400/401/403/404/500 with body {code, iServerCode, message, ticketId, subErrors[]}
   ver: NEW in the 2025->2026 platform generation. Wayback snapshot of the SAME spec URL from 2025-04-02 (platform version '2024') has NO patch method on /subscriptions/{id}, NO SubscriptionPatch schema, and NO softDisabled property anywhere. Current demo spec (version '2026') has all three. Not listed in t
[verified] verify_patch_support_on_client_build: GET /openapi.json
   resp: OpenAPI 3.0.1 JSON (~6 MB on demo). Check: paths['/api/subscriptions/{id}'] contains 'patch' with operationId 'patchSubscription'; components.schemas.SubscriptionPatch.properties.softDisabled exists; components.schemas.DeliveryProperties.properties.softDisabled exists (needed for the PUT fallback). info.version tells you the platform generation ('2026' = has it; '2024' = does not)
   ver: Spec location stable across the 2024 and 2026 generations (Wayback has the same URL serving the 2024-version spec in Aug 2024 and Apr 2025).
[verified] get_subscription_full: GET /subscriptions/{id}
   headers: X-MSTR-ProjectID (required in practice; mstrio-py always sends it); X-MSTR-ClientVersion (optional; mstrio-py sends '25.06')
   query: fields (optional top-level whitelist)
   resp: 200 OK, Subscription object: {id, name, multipleContents, editable, allowDeliveryChanges, allowPersonalizationChanges, allowUnsubscribe, sendNow, dateCreated, dateModified, owner, schedules, contents, recipients, delivery{mode, expiration, softDisabled, useRecipientTimezone, contactSecurity, email|file|ftp|cache|mobile|historyList|..., notificationEnabled, personalNotification}, lastRun, nextDelivery, alert}. On the 2026 spec, delivery.softDisabled is a readable boolean (default false, NOT write
   ver: delivery.softDisabled only exists in the response on 2026-generation builds; the 2024-generation DeliveryProperties schema has no such field.
[verified] update_subscription_put_fallback: PUT /subscriptions/{id}
   headers: X-MSTR-ProjectID (required in practice; mstrio-py always sends it); X-MSTR-ClientVersion (optional; mstrio-py sends '25.06')
   query: fields (optional)
   body: Full Subscription object (same shape as GET response) with delivery.softDisabled set to true. Send the raw GET JSON back with only that one field flipped — do NOT round-trip through mstrio-py's Subscription/Delivery classes, because mstrio-py's Delivery model (distribution_services/subscription/delivery.py) has NO softDisabled attribute and would silently drop it.
   resp: 200 OK with the updated Subscription object (spec); errors 400/401/500 with {code, iServerCode, message, ticketId}
   ver: CRITICAL caveat: PATCH support and the delivery.softDisabled field arrived TOGETHER in the 2026 generation (both absent from the 2024-version spec). So if the client's build lacks the PATCH, the PUT fallback almost certainly cannot soft-disable either — the field would not exist in the schema. The f
   NOTES: GAP RESOLUTION SUMMARY. (1) The prior single-source finding is now multi-checked: I re-fetched the live demo spec as raw JSON (not via the Swagger UI summary) and machine-parsed it — PATCH /api/subscriptions/{id}, opId patchSubscription, body SubscriptionPatch{name?, softDisabled?(writeOnly)}, 204 No Content, X-MSTR-AuthToken required + X-MSTR-ProjectID formally optional. Exact spec URL for the client-build probe is https://<server>/MicroStrategyLibrary/api/openapi.json (verified from the Swagger UI JS bundle; /api-docs/openapi.json 404s). (2) The client's own spec could NOT be fetched from here — their MSTR runs in the vendor's AWS behind PrivateLink, internal-only — so implement the openapi.json capability probe at app startup/deploy and cache the result. (3) Dating: Wayback snapshot 2025-04-02 of the same demo spec (platform version '2024') lacks PATCH, SubscriptionPatch, and softDisabled entirely; current spec (version '2026') has all three; the feature is absent from official what's-new (through May 2026), official workflow docs, and mstrio-py through 11.6.6.101 (2026-06-18). It is a spec-only endpoint of the 2026 generation — expected present on a March 2026 build, but unconfirmed until probed. (4) Body-shape gotcha: PATCH takes softDisabled at TOP LEVEL ({\"softDisabled\": true}); GET/PUT carry it NESTED at delivery.softDisabled. (5) ProjectID: spec says optional on PATCH/GET/PUT, but subscriptions are project objects and mstrio-py unconditionally sends X-MSTR-ProjectI
============================== GAPFILL AREA: gap:resume_subscription
[verified] resume_subscription_patch: PATCH /subscriptions/{id}
   headers: X-MSTR-AuthToken (required); X-MSTR-ProjectID (send always for project-scoped subscriptions; spec lists it on this op without required flag)
   body: {"softDisabled": false}  — schema components/SubscriptionPatch: { name?: string(1..250), softDisabled?: boolean (writeOnly) }; all fields optional; to resume send softDisabled=false, to pause true
   resp: 204 No Content on success (no body). Errors: 400/401/403/404/500. State must be read back via GET /subscriptions/{id} at delivery.softDisabled — the PATCH field is writeOnly so it never echoes back at top level.
   ver: Present in the current 2026-generation spec (matches client's March 2026 release). NOT wrapped by mstrio-py (master branch mstrio/api/subscriptions.py has no PATCH and no pause/resume method — confirmed by fetching raw source). Existence+shape verified in the vendor's live 2026 OpenAPI spec; the beh
[verified] resume_subscription_put_fallback: PUT /subscriptions/{id}
   headers: X-MSTR-AuthToken (required); X-MSTR-ProjectID; X-MSTR-ClientVersion: 25.06 (optional; mstrio-py sends it)
   query: fields (optional)
   body: Full components/Subscription object (required: name, delivery). Fetch via GET /subscriptions/{id}, set delivery.softDisabled=false, PUT the whole object back. delivery is components/DeliveryProperties which includes softDisabled: boolean (default false).
   resp: 200 OK with updated Subscription body (not 204). Errors: 400/401/500.
   ver: PUT is the long-standing v1 update path and the only update mstrio-py wraps; no v2 variant of subscription update exists (only send has /api/v2/subscriptions/{id}/send). Caveat for the fallback: PUT replaces the object, so the GET->mutate->PUT cycle must preserve schedules, contents, recipients, and
[verified] get_subscription: GET /subscriptions/{id}
   headers: X-MSTR-AuthToken (required); X-MSTR-ProjectID (required in practice; mstrio-py always sends it); X-MSTR-ClientVersion: 25.06 (optional; mstrio-py sends it)
   query: fields (optional)
   resp: 200 OK -> components/Subscription: { id, name, editable, allowDeliveryChanges, allowUnsubscribe, dateCreated, dateModified, owner, schedules[], contents[], recipients[], delivery{ mode, ..., softDisabled: boolean (default false) }, lastRun, nextDelivery, ... }. Paused state reads as delivery.softDisabled=true; resumed = false/absent(default false).
   ver: Dual-source verified (mstrio-py master + live 2026 spec). Related: DependentSubscription objects from GET /dependentSubscriptions expose the same flag via SimpleDeliveryProperties.softDisabled.
   NOTES: Gap resolution status: the single-source risk is substantially reduced but not fully closed. NEW evidence beyond the prior research: (1) re-verified PATCH /api/subscriptions/{id} + SubscriptionPatch on the vendor demo server's CURRENT spec, titled 'Strategy REST 2026' (same generation as the client's March 2026 release), not an old snapshot; (2) the spec marks SubscriptionPatch.softDisabled as writeOnly:true, proving the top-level-write vs delivery.softDisabled-read asymmetry is intentional API design — resolving the flagged inconsistency without needing a round-trip to explain it; (3) the vendor's official Postman workspace (index embedded in the local clone of MicroStrategy/rest-api-docs at D:\WorkspaceAI\StrategyAI\references\rest-api-docs\postman.json) confirms PATCH is a supported verb on the subscriptions resource ('Change subscription owner' = PATCH /subscriptions/{id}/owner, operationId changeSubscriptionOwner; batch variant PATCH /subscriptions/owner), though the softDisabled PATCH itself appears only in the OpenAPI spec; (4) confirmed mstrio-py (master) does NOT wrap the PATCH and has no pause/resume — so the PUT full-object path is the SDK-sanctioned fallback. What remains UNVERIFIED: the live behavioral round-trip on the client's environment ((a) 204, (b) GET shows delivery.softDisabled=false, (c) delivery actually resumes on next schedule) — impossible from this workspace: no client credentials (.env absent) and the client's MSTR is internal/PrivateLink-only; wri
============================== GAPFILL AREA: gap:get_refresh_status
[verified] get_job_v2: GET /v2/monitors/jobs/{id}
   query: fields (optional comma-separated top-level field whitelist)
   resp: 200 JobDetailedInfo: {id: string (GUID, the path param), jobId: integer (legacy numeric), status: string ENUM['ready','executing','waiting','completed','error','canceling','stopped','waiting_on_governor','waiting_for_autoprompt','waiting_for_project','waiting_for_cache','waiting_for_children','waiting_for_results','loading_prompt','resolving_destination','delivering','exporting','cache_ready','waiting_for_di_file','waiting_for_conflict_resolve','step_pausing'], type: ENUM['interactive','subscrip
   ver: v2 is current (11.3.3+; mstrio only uses v1 get_job for iServer exactly 11.3.2, where it emulates a single-job GET by listing /monitors/jobs per node and filtering client-side). v1 GET /monitors/jobs/{id} does not exist as a real endpoint. Spec version '2026' confirms present in the March 2026 relea
[verified] get_cube_status: GET /cubes/{cubeId}
   headers: X-MSTR-ProjectID (required)
   resp: ACTUAL HTTP VERB IS HEAD (not listed in this schema's method enum). 204 No Content. Cube state returned in response header X-MSTR-CubeStatus as an integer bit vector (EnumDSSCubeStates). Bits (mstrio CubeStates): PROCESSING=1, ACTIVE=2, PERSISTED=4, DIRTY_INFO=8, DIRTY=16, LOADED=32, READY=64, LOAD_PENDING=128, UNLOAD_PENDING=256, PENDING_FOR_ENGINE=512, IMPORTED=1024, FOREIGN=2048. Errors: 400/500 error body, 401 unauthorized. NOTE: plain GET on the same path is a different operation (getDefini
   ver: HTTP method is HEAD (schema here lacks HEAD, reported under GET — do NOT call with GET for the header check). Unversioned v1 path; present unchanged in the 2026 spec. The X-MSTR-CubeStatus header is not declared in the OpenAPI 204 response object but is documented and consumed by the vendor SDK.
[verified] publish_cube: POST /v2/cubes/{cubeId}
   headers: X-MSTR-ProjectID (required)
   query: fields (optional)
   resp: 202 Accepted ('Request accepted for processing'), body JobId schema: {id: string (monitor job GUID — use this for GET /v2/monitors/jobs/{id}), jobId: integer (legacy numeric id), instanceId: string}. Errors 400/401/500 -> error schema.
   ver: v2 is current; mstrio uses v2 exclusively.
[verified] list_jobs_v2: GET /v2/monitors/jobs
   query: nodeName (must be first param per mstrio comment), status, type, objectId, objectType, projectId, projectName, user, description, puName, subscriptionType, subscriptionRecipient, memoryUsage (gte:/lte:), elapsedTime (gte:/lte:), sortBy, fields
   resp: 200 {jobs: [JobDetailedInfo...]} — same status enum as get_job_v2. Lists ACTIVE jobs on a node only.
   ver: v2 current since 11.3.3; v1 GET /monitors/jobs returns a reduced field set and is only used by mstrio for iServer 11.3.2.
[verified] cancel_job_v2: DELETE /v2/monitors/jobs/{id}
   resp: 200/204 on success; error schema {code, iServerCode, message, ticketId} otherwise.
   ver: v2 current; v1 DELETE /monitors/jobs/{id} and bulk POST /v2/monitors/cancelJobs also exist.
   NOTES: STATUS ENUM RESOLVED (verified in two independent vendor sources): the job body 'status' field wire values are lowercase snake_case strings — ready, executing, waiting, completed, error, canceling, stopped, waiting_on_governor, waiting_for_autoprompt, waiting_for_project, waiting_for_cache, waiting_for_children, waiting_for_results, loading_prompt, resolving_destination, delivering, exporting, cache_ready, waiting_for_di_file, waiting_for_conflict_resolve, step_pausing. Terminal set = {completed, error, stopped} (mstrio job_monitor.py refresh_status stable_states); everything else = still running/queued. Failure detail while job is retrievable: status=='error' plus errorMessage (string) and errorTime fields in the same body. PURGE BEHAVIOR: the endpoint is documented as serving ACTIVE jobs only ('Get detailed information on a specific active job'); once finished the job is purged and GET returns 404 (mstrio also handles 500 for this case — its refresh_status catches IServerError http_code in {404, 500} and force-sets status to COMPLETED when last-known status was non-terminal). This is vendor-confirmed behavior from SDK source + spec wording; NOT re-confirmed against a live publish job in this research pass (no client credentials; writing to the vendor demo server was out of scope) — so a poller must treat 'job gone' as 'finished, outcome unknown', never as 'succeeded'. RECOMMENDED POLLER: (1) POST /v2/cubes/{cubeId} -> 202, take body.id (string GUID, NOT the integer jobId); 