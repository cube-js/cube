# Access policies

```yaml
# cube.yml — reusable policies + project-wide defaults
access_policies:
  finance:
    groups: [finance]

  internal:
    groups: [analyst, manager, admin]

  org_admin:
    user_attribute: is_org_admin
    values: ["true"]
```

```yaml
# views/sales_pipeline.yml — cube/view-level access control
views:
  - name: sales_pipeline
    # Who can query this view at all (references reusable policies)
    required_access_policies:
      any_of: [internal, sales]

    # Which rows they see
    access_filters:
      - member: user_id
        operator: equals
        values: ["{ userAttributes.userId }"]
        # Filter applies only to sales reps; internal & admin skip it
        apply_if_access_policies: [sales]
```

```yaml
# cubes/order_items.yml — member-level rules
cubes:
  - name: order_items
    dimensions:
      - name: revenue
        sql: sale_price
        type: number
        required_access_policies: [finance]   # hard deny

      - name: cost
        sql: cost
        type: number
        mask_unless_access_policies: [finance]         # soft mask
        mask: -1
```

Three layers:

| Layer | Keyword(s) |
|---|---|
| Project | `access_policies` (registry of reusable policies) |
| Cube / view | `required_access_policies` (who can query), `access_filters` (which rows) |
| Member | `required_access_policies`, `mask_unless_access_policies`, `mask` |

Cubes, views, and members are public by default. Policies only
restrict; they never grant access the underlying SQL connection
doesn't already have.

---

## Reusable policies

Named, predicate-only policies registered under top-level
`access_policies`. Each policy resolves to true/false for a given user
based on its target parameters.

```yaml
# cube.yml
access_policies:
  # Match against a group from contextToGroups()
  finance:
    groups: [finance]

  # Match a user attribute against a value list
  nw_region:
    user_attribute: region
    values: [washington, idaho, oregon, wyoming, alaska]

  org_admin:
    user_attribute: is_org_admin
    values: ["true"]

  # Arbitrary boolean expression over userAttributes / securityContext
  privacy_trained:
    conditions:
      - if: "{ userAttributes.has_completed_privacy_training }"

  # Multiple parameters on the same policy compose with AND
  finance_and_trained:
    groups: [finance]
    conditions:
      - if: "{ userAttributes.has_completed_privacy_training }"
```

Once defined, policies are referenced by name from
`required_access_policies` on cubes/views, `required_access_policies`
and `mask_unless_access_policies` on members, and
`apply_if_access_policies` on access filters.

### Policy expressions

Anywhere a reusable policy reference is accepted
(`required_access_policies:`, `mask_unless_access_policies:`,
`apply_if_access_policies:`), the value can be either a plain list of policy
names or a structured expression. A plain list is interpreted as
**AND** — the user must satisfy every listed policy.

```yaml
# Single policy
required_access_policies: [internal]

# AND across array entries — user must satisfy ALL listed policies
required_access_policies: [internal, privacy_trained]

# OR — user must satisfy at least one of the listed policies
required_access_policies:
  any_of: [internal, sales]

# Mixed: (marketing OR finance) AND nw_region
required_access_policies:
  all_of: [nw_region]
  any_of: [marketing, finance]

# Negation: deny if user satisfies any of these policies
required_access_policies:
  all_of: [internal]
  none_of: [contractor]
```

When more than one of `all_of`, `any_of`, and `none_of` is present in
the same expression, they compose with AND.

---

## Required access policies on cubes and views

Cubes and views control who can query them with
`required_access_policies`. The value is a [policy
expression](#policy-expressions) — a list of reusable policy names
(AND), or a structured `all_of` / `any_of` / `none_of` block.

```yaml
views:
  - name: orders_view
    required_access_policies:
      any_of: [internal, sales]
```

Defaults:

- Omitted (or empty) `required_access_policies` → the cube/view is open
  to everyone.
- A non-empty `required_access_policies` that the user doesn't satisfy
  → access is denied. No need to write an explicit deny.

All targeting (groups, user attributes, conditions) lives only inside
[reusable policy](#reusable-policies) definitions. Cubes and views
just reference policies by name.

---

## Access filters

`access_filters` is a list of row-level grants applied **after** the
user has passed `required_access_policies`.

Filters use the same format as REST (JSON) API query filters,
including all standard [filter operators](#) and the `and` / `or`
boolean operators for conjunctive conditions inside a single filter.

### How filters combine

For a given user, only filters whose
[`apply_if_access_policies`](#apply_if_access_policies) matches that
user are **active**. Then:

- **No filter active** → no row restriction; the user sees every row
  that passed `required_access_policies`.
- **One filter active** → its row condition restricts the result.
- **Multiple filters active** → results combine with **OR** (union of
  allowed rows). Each filter declares an additional set of rows the
  user is allowed to see.

```yaml
views:
  - name: deals
    required_access_policies:
      any_of: [internal, sales]

    access_filters:
      # Sales reps: their own deals
      - member: user_id
        operator: equals
        values: ["{ userAttributes.userId }"]
        apply_if_access_policies: [sales]

      # Sales reps in their own region
      - member: region
        operator: equals
        values: ["{ userAttributes.region }"]
        apply_if_access_policies: [sales]
```

A pure-`sales` user sees `user_id == self OR region == self.region` —
the union, not the intersection. **For conjunctive conditions inside a
single grant, use the `and` boolean operator** within that filter (see
the [Sales deals example](#sales-deals-with-regional-closed-won-access)).

`internal` users (who don't match any filter's
`apply_if_access_policies`) see everything — there's no active
filter restricting them.

### `apply_if_access_policies`

Each filter accepts an optional `apply_if_access_policies` parameter
that scopes **when the filter is active**. The value is a [policy
expression](#policy-expressions) (plain list = AND, or
`all_of` / `any_of` / `none_of`).

- Omitted → filter is active for everyone who passed
  `required_access_policies`.
- Plain list / `all_of` → active only for users who satisfy every
  listed policy.
- `any_of` → active for users who satisfy any one of them.
- `none_of` → active for users who satisfy none of them.

---

## Member access

Three parameters on a dimension or measure control how it's exposed.
Pick **one** of the two access modes per member; `mask` only matters
with the soft-mask mode.

| Parameter | Effect when not satisfied | Pairs with |
|---|---|---|
| `required_access_policies` | Hard deny. Member hidden from metadata; queries that reference it fail. | — |
| `mask_unless_access_policies` | Soft mask. Member stays queryable; returns `mask` instead of real data. | `mask` |
| `mask` | The value returned by `mask_unless_access_policies`. Defaults to `MD5(<value>)` for strings, `NULL` otherwise. | `mask_unless_access_policies` |

If both `required_access_policies` and `mask_unless_access_policies`
are set on the same member, `required_access_policies` is checked
first — strict deny wins.

### Strict access with `required_access_policies`

```yaml
cubes:
  - name: order_items
    dimensions:
      - name: status
        sql: status
        type: string

      - name: revenue
        sql: sale_price
        type: number
        required_access_policies: [finance]

      - name: margin
        sql: ${order_items.sale_price} - ${products.cost}
        type: number
        required_access_policies: [finance, org_admin]   # AND

    measures:
      - name: total_sale_price
        sql: sale_price
        type: sum
        required_access_policies: [finance]
```

`required_access_policies` accepts the same expression forms as
[policy expressions](#policy-expressions) above — a plain list (AND) or
a structured `all_of` / `any_of` / `none_of` block.

### Soft masking with `mask_unless_access_policies`

Reads literally: mask the value **unless** the user satisfies a listed
policy. The field stays queryable for everyone, so joins, group-bys,
and `count_distinct` keep working across the user base.

```yaml
cubes:
  - name: users
    dimensions:
      - name: email
        sql: email
        type: string
        mask_unless_access_policies: [support, marketing]
        # No explicit `mask` → defaults to MD5(email) for non-listed users

      - name: full_name
        sql: full_name
        type: string
        mask_unless_access_policies: [support]
        mask:
          sql: "CONCAT('***', RIGHT({CUBE}.full_name, 4))"
```

`mask_unless_access_policies` accepts the same expression forms as `required_access_policies`.

### Mask values

Default mask: `MD5(<value>)` for strings (deterministic — joins and
grouping still work), `NULL` for numbers/booleans/times. Override
per-member:

```yaml
dimensions:
  - name: revenue
    sql: sale_price
    type: number
    mask_unless_access_policies: [finance]
    mask: -1                    # static value

  - name: phone
    sql: phone
    type: string
    mask_unless_access_policies: [support]
    mask:
      sql: "CONCAT('+', LEFT({CUBE}.phone, 2), '-***-****')"

measures:
  - name: revenue_count
    type: count
    mask_unless_access_policies: [finance]
    mask: 0
```

Globally override defaults via `CUBEJS_ACCESS_POLICY_MASK_STRING`,
`_NUMBER`, `_BOOLEAN`, `_TIME`.

> SQL masks (`mask: { sql: "..." }`) on measures are not applied in
> ungrouped queries (e.g. `SELECT *` via the SQL API), because the SQL
> expression typically references columns that aren't meaningful in a
> per-row context. Static masks (`mask: -1`, `mask: 0`) are always
> applied. To mask a measure dynamically in ungrouped queries, define
> a masked dimension and reference it instead.

---

## Evaluation rules

For each request, in order:

1. **`required_access_policies`** — the cube/view's
   `required_access_policies` expression must evaluate to true for the
   user. Empty / unset → cube/view is open.
2. **Member checks** — for every member referenced:
   - If `required_access_policies` isn't satisfied, the request is denied.
   - Otherwise, if `mask_unless_access_policies` isn't satisfied, `mask`
     is returned in place of the real value.
3. **`access_filters`** — for each filter in the cube/view's
   `access_filters`, check whether `apply_if_access_policies` matches
   the user (filters with no `apply_if_access_policies` always match).
   Active filters are combined with **OR** — the user sees the union
   of all rows allowed by any active filter. If no filter is active
   for the user, no row restriction is applied.

### Composition with `public` and `query_rewrite`

- A member's `required_access_policies` is combined with the `public`
  parameter on the member using AND. Both must allow access.
- The OR'd result of `access_filters` is combined with filters from
  `query_rewrite` using AND. `query_rewrite` is always restrictive:
  it narrows whatever the access filters allow.

### Composition through views

- **Member access** on a view is independent of the underlying cubes.
  A member exposed by a view uses the view's own `required_access_policies` /
  `mask_unless_access_policies` / `mask`; the underlying cube member's parameters are
  not combined. This mirrors SQL column visibility — once a column is
  exposed by a view, the view's grants are authoritative.
- **Access filters** on a view compose with the underlying cubes.
  Filters from both layers apply.

---

## Mapping users to groups

Cube cloud platform maps authenticated users to groups automatically.
For Cube Core / direct Core Data API auth, provide `context_to_groups`
(Python) or `contextToGroups` (JS):

```python
# cube.py
from cube import config

@config('context_to_groups')
def context_to_groups(ctx: dict) -> list[str]:
    return ctx['securityContext'].get('groups', ['default'])
```

```javascript
// cube.js
module.exports = {
  contextToGroups: ({ securityContext }) => {
    return securityContext.groups || ['default'];
  }
};
```

---

## Using `securityContext` directly

`userAttributes` is a Cube cloud platform convenience. With Cube Core
or direct Core Data API auth, use `securityContext` instead:

```yaml
access_policies:
  region_user:
    user_attribute: securityContext.region
    values: ["*"]

  manager:
    user_attribute: securityContext.groups
    values: [manager]

cubes:
  - name: orders
    required_access_policies: [manager]

    access_filters:
      - member: country
        operator: equals
        values: ["{ securityContext.country }"]
```

---

## Worked examples

### Example cases

Sample `deals` data referenced by the examples below:

| amount | region | stage | name |
|---|---|---|---|
| $45,000 | North America | Closed Won | Acme Corp Renewal |
| $190,000 | EMEA | Closed Won | Wayne Enterprises |
| $67,500 | APAC | Closed Won | Soylent Corp |
| $310,000 | North America | Closed Won | Cyberdyne Systems |
| $128,500 | EMEA | Negotiation | Globex Expansion |
| $12,000 | North America | Prospecting | Initech Pilot |
| $85,000 | APAC | Qualified | Umbrella Holdings |
| $250,000 | EMEA | Proposal | Stark Industries |

Three user groups: users, sales, sales_regional_managers.

Three users:

| User | Groups | User attributes |
|---|---|---|
| Artyom | `users` | — |
| Pavel | `users`, `sales` | — |
| Alex | `users`, `sales`, `sales_regional_managers` | `region: EMEA` |

### Sales deals with regional Closed Won access

Goal:

- Artyom sees no deals at all.
- Pavel sees every deal **except** Closed Won.
- Alex sees everything Pavel sees, **plus** Closed Won deals in his
  own region (EMEA).

```yaml
# cube.yml
access_policies:
  sales:
    groups: [sales]

  sales_regional_manager:
    groups: [sales_regional_managers]
```

```yaml
# views/deals.yml
views:
  - name: deals

    # Without `sales`, the view isn't queryable at all → Artyom denied
    required_access_policies: [sales]

    access_filters:
      # Active for everyone in sales (including regional managers):
      # see all non-Closed-Won deals.
      - member: stage
        operator: notEquals
        values: ["Closed Won"]
        apply_if_access_policies:
          any_of: [sales, sales_regional_manager]

      # Active only for regional managers: also see deals in their region
      # (regardless of stage, including Closed Won).
      - member: region
        operator: equals
        values: ["{ userAttributes.region }"]
        apply_if_access_policies: [sales_regional_manager]
```

How the two filters combine (per [evaluation rules](#evaluation-rules)):

- **Pavel** has only filter 1 active → `stage != "Closed Won"`.
- **Alex** has both active → `stage != "Closed Won" OR region == "EMEA"`
  (the union — the **OR** combination of access filters).

| User | Effective row condition | Visible deals |
|---|---|---|
| Artyom | denied — fails `required_access_policies: [sales]` | none |
| Pavel | `stage != "Closed Won"` | Globex, Initech, Umbrella, Stark |
| Alex | `stage != "Closed Won" OR region == "EMEA"` | Globex, Initech, Umbrella, Stark, **Wayne Enterprises** |

Per-deal visibility:

| Deal | Stage | Region | Artyom | Pavel | Alex |
|---|---|---|:-:|:-:|:-:|
| Acme Corp Renewal | Closed Won | NA | — | — | — |
| Wayne Enterprises | Closed Won | EMEA | — | — | ✓ |
| Soylent Corp | Closed Won | APAC | — | — | — |
| Cyberdyne Systems | Closed Won | NA | — | — | — |
| Globex Expansion | Negotiation | EMEA | — | ✓ | ✓ |
| Initech Pilot | Prospecting | NA | — | ✓ | ✓ |
| Umbrella Holdings | Qualified | APAC | — | ✓ | ✓ |
| Stark Industries | Proposal | EMEA | — | ✓ | ✓ |

#### Field-level access: `region` is regional-manager-only

Layering [member-level access](#member-access) on top of the same
example: `region` is a regional-manager-only field. Pavel still sees
the same 4 deals as before — but can no longer reference `region` in
any query. Alex is unchanged.

```yaml
# cubes/sales_deals.yml
cubes:
  - name: sales_deals
    sql_table: CRM.DEALS

    dimensions:
      - name: name
        sql: name
        type: string

      - name: amount
        sql: amount
        type: number

      - name: stage
        sql: stage
        type: string

      # Hard deny for anyone who isn't a regional manager:
      # the field is hidden from metadata and queries that reference
      # it fail.
      - name: region
        sql: region
        type: string
        required_access_policies: [sales_regional_manager]
```

Per-user field visibility:

| Field | Artyom | Pavel | Alex |
|---|:-:|:-:|:-:|
| `name` | — | ✓ | ✓ |
| `amount` | — | ✓ | ✓ |
| `stage` | — | ✓ | ✓ |
| `region` | — | — | ✓ |

> Artyom shows `—` across the board because he fails the view-level
> `required_access_policies: [sales]` — the member-level rules never
> get a chance to apply to him.

The `region`-scoped access filter for Alex still works: the filter is
evaluated by the engine using the user's attribute, and Alex's policy
(`sales_regional_manager`) grants him read access to the `region`
member the filter references.

### Region-scoped access on a folder of views

Pattern: a folder of related views (here, a `SUPPLY_CHAIN` folder
containing four views) is exposed to two groups. One group sees all
rows; the other sees only rows for North America.

```yaml
# cube.yml
access_policies:
  supply_chain_full:
    groups: [cube_core_supply_chain]

  supply_chain_na:
    groups: [cube_core_supply_chain_na]
```

```yaml
# views/supply_chain/*.yml — same shape on view_1, view_2, view_3, view_4
views:
  - name: view_1
    # Either policy grants access to all four views in the folder
    required_access_policies:
      any_of: [supply_chain_full, supply_chain_na]

    # Region filter only kicks in for the NA-scoped policy;
    # supply_chain_full users skip it and see every row.
    access_filters:
      - member: region
        operator: equals
        values: ["North America"]
        apply_if_access_policies: [supply_chain_na]
```

| User | Groups | Effect on `view_1`–`view_4` |
|---|---|---|
| User 1 | `cube_core_supply_chain` | matches `supply_chain_full` → sees all rows, no filter |
| User 2 | `cube_core_supply_chain_na` | matches `supply_chain_na` → sees only `region = "North America"` rows |
| Anyone else | — | denied (neither policy matches) |

The `apply_if_access_policies` scope on the filter is what lets the two
groups share the same view definitions while seeing different data.

---

### Ecommerce demo

[Ecommerce demo](#) with three views and a soft-masked `cost` field.
All access logic in `cube.yml`; views only reference policies by name.

```yaml
# cube.yml
access_policies:
  finance:
    groups: [finance, admin]

  org_admin:
    groups: [admin]

  sales:
    groups: [sales]

  internal:
    groups: [analyst, manager, admin]
```

```yaml
# views/order_revenue.yml
views:
  - name: order_revenue
    cubes:
      - join_path: order_items
        includes: [status, created_at, total_sale_price, count]
      - join_path: order_items.products
        includes: [brand, category, cost]
      - join_path: order_items.users
        includes: [country, traffic_source]

    # No required_access_policies → open to everyone. cost is
    # soft-masked for non-finance users (see products cube below).
    access_filters:
      # Region filter applies to everyone except org admins
      - member: country
        operator: equals
        values: ["{ userAttributes.region }"]
        apply_if_access_policies:
          none_of: [org_admin]
```

```yaml
# cubes/products.yml
cubes:
  - name: products
    sql_table: ECOMMERCE.PRODUCTS

    dimensions:
      - name: cost
        sql: cost
        type: number
        mask_unless_access_policies: [finance]
        mask: -1
```

```yaml
# views/customer_pii.yml
views:
  - name: customer_pii
    cubes:
      - join_path: users
        includes: [email, full_name, country]

    required_access_policies: [org_admin]
```

```yaml
# views/sales_pipeline.yml
views:
  - name: sales_pipeline
    cubes:
      - join_path: orders
        includes: [status, count]
      - join_path: orders.users
        includes: [city, country]

    # Sales reps and internal users can both query the view
    required_access_policies:
      any_of: [sales, internal]

    access_filters:
      # Sales reps: only their own deals, only in their region
      - and:
          - member: user_id
            operator: equals
            values: ["{ userAttributes.userId }"]
          - member: country
            operator: equals
            values: ["{ userAttributes.region }"]
        apply_if_access_policies: [sales]

      # Internal non-admins: all deals, but only in their region
      - member: country
        operator: equals
        values: ["{ userAttributes.region }"]
        apply_if_access_policies:
          all_of: [internal]
          none_of: [org_admin, sales]
```

| User profile | `order_revenue` | `customer_pii` | `sales_pipeline` |
|---|---|---|---|
| Sales rep (`groups: [sales]`, region=`CA`) | open, filtered to `country=CA`; `cost` masked | denied | own deals, filtered to `country=CA` |
| Analyst (`groups: [analyst]`, region=`CA`) | open, filtered to `country=CA`; `cost` masked | denied | all deals, filtered to `country=CA` |
| Finance (`groups: [finance]`, region=`US`) | open, filtered to `country=US`; `cost` visible | denied | denied |
| Admin (`groups: [admin]`) | open, region filter bypassed; `cost` visible | full access | all deals, region filter bypassed |

---

## Reference

- [`access_policies`](#) — project-level reusable policy registry
- [`required_access_policies`](#) — cube/view-level access requirement (references reusable policies)
- [`access_filters`](#) — cube/view-level row filters
- [`apply_if_access_policies`](#) — scope when an access filter applies; omit to apply to everyone
- [`required_access_policies`](#) — strict member-access requirement
- [`mask_unless_access_policies`](#) — soft mask: members stay queryable but return `mask` unless the user satisfies a listed policy
- [`mask`](#) — value returned in place of real data for masked or denied members
- [Security context](#) — `userAttributes` and `securityContext`
- [`contextToGroups`](#) — mapping users to groups

---

## Potential gaps

This is a working design doc. Open questions and rough edges to settle
before this can ship.

### Functional gaps

- **No project-level defaults.** We dropped `default_access_policy` /
  `default_access_filters`. The "every view in this project requires
  the `analyst` policy" pattern requires copy-paste on every view.
  Same for "every view filters by region". Common request — likely
  needs to come back as a follow-up.
- **No cube-level "mask all members unless..." shortcut.** Today every
  PII column needs its own `mask_unless_access_policies` and `mask`.
  For a 50-column PII table, that's 50 identical declarations. Some
  options: a cube-level `mask_unless_access_policies` that propagates
  to every member, or a `tag` mechanism for grouping members.
- **No per-group masks.** A field with `mask_unless_access_policies:
  [finance, marketing]` returns the **same** `mask` value to non-finance
  AND non-marketing users. There's no way to give marketing a partial
  view (e.g. last 4 digits) while keeping finance unrestricted and
  everyone else fully masked.
- **No "hide silently" mode.** `required_access_policies` currently
  errors on denial — the user knows the field exists. For sensitive
  PII this can leak schema information. No opt-in for "drop silently
  from the result instead of erroring".
- **No dynamic / computed policies.** All policies are static YAML.
  No way to compute group membership at request time from an external
  service (org chart, entitlements API). The escape hatch is
  `contextToGroups` returning derived groups, but that runs once
  per request, not per policy evaluation.
- **Segments and hierarchies under-specified.** `required_access_policies`
  / `mask_unless_access_policies` are documented for dimensions and
  measures. Segments and hierarchies are mentioned once in the intro
  but not shown in any example. Need to confirm semantics for both.

### Operational gaps

- **No debugging tools.** When a user reports "I can't see view X",
  there's nothing in the spec for "show me which policies the user
  satisfies and why this view denied them". Without an impersonate or
  dry-run mode, every access bug requires log diving.
- **No audit story.** Denials are silent at the spec level. Production
  RBAC needs an audit trail (who got denied accessing what, when, by
  which policy). Needs a logging hook or a `/audit` endpoint.
- **Caching / pre-aggregation interaction undefined.** Pre-aggregations
  are user-context-aware via `securityContext`, but it's not clear how
  per-user `apply_if_access_policies` filters affect cache keys and
  hit rates. Worst case: every user gets a unique cache entry.
- **Hot-reload / versioning unspecified.** What happens to in-flight
  queries when a view's `required_access_policies` changes? Are cached
  results invalidated? Does the new policy apply to next-page-load only?

### Spec ambiguities

- **Cube vs view member composition.** "View grants are authoritative"
  — but what if the view doesn't redeclare `required_access_policies`
  on a member that has them on the underlying cube? Are the cube's
  policies dropped (potential PII leak), or carried through (violates
  "view is authoritative" rule)? Current text implies dropped. Needs
  a clearer stance and probably a lint warning.
- **`public:` ↔ `required_access_policies` interaction.** Eval rules
  say they compose with AND on members. But what about
  `public: false` on a whole cube/view? Does it short-circuit
  `required_access_policies`, or run alongside? What does
  `public: true` + `required_access_policies: [admin]` mean?
- **Registry scope with multiple `cube.yml` files.** Where do reusable
  policies live in a multi-team / monorepo setup with several model
  modules? Project-global, or per-module? Name collisions?
- **AI agent identity.** Cube agents (Analytics Chat, ad-hoc queries)
  query the model on behalf of users. Do agents resolve to the
  invoking user's policies, get their own identity, or have a
  bypass? Not addressed.
- **Multi-tenancy / cross-data-source.** `access_filters` adds WHERE
  clauses but doesn't switch underlying data sources. Customers
  isolating tenants by schema/database typically use `dataSource()`
  + `securityContext`. Relationship between that pattern and the
  new access policies is undefined.

### UX risks

- **New fields default to public.** A developer adding a new column
  to a cube that has `mask_unless_access_policies` on its sibling
  PII fields gets full public exposure on the new field — they have
  to remember to add the parameter. Risky for PII-heavy tables.
  See the "mask all members" gap above; one solves the other.
- **`values:` template syntax undocumented.** Examples use
  `values: ["{ userAttributes.userId }"]` — what's the full grammar?
  Just attribute references? Expressions? Function calls? Reusable
  helpers? Needs a dedicated reference page.
- **OR-combination of access filters is unfamiliar to SQL users.**
  Each `access_filter` declares an additional set of allowed rows; the
  user sees the union of all matching filters. This is the row-level
  grant model (Omni-style), not the SQL `WHERE` model where each
  filter narrows further. Customers expecting "add a filter to
  restrict" will be surprised when adding a second filter widens
  visibility instead. Mitigations: lint warning when two filters with
  overlapping `apply_if_access_policies` would broaden access, and
  prominent "see also: `and` operator" callouts on every multi-filter
  example.
- **Cube vs view: is putting policies on cubes ever a good idea?**
  Customers typically query views, not cubes. Putting
  `required_access_policies` on a cube guards the underlying SQL but
  doesn't show up to view consumers in the same shape. Worth taking
  a position: "policies belong on views; on cubes only as defense in
  depth" or similar.

### Naming nits

- **`apply_if_access_policies` is a mouthful** for what's likely the
  most-touched parameter on a filter. `apply_if` alone would read
  fine in context (the value is obviously a policy expression).
- **`mask_unless_access_policies` similarly long.** `mask_unless`
  would be enough. The `_access_policies` suffix is informative but
  adds 17 characters everywhere it appears.
- **`required_access_policies` is overloaded** — it appears on
  cubes/views (gates query), on members (hard deny), and means
  slightly different things in each context. A second name like
  `requires_access_policies` (for query-gating) vs
  `required_access_policies` (for member-level) might disambiguate,
  but at the cost of doubling the surface.
