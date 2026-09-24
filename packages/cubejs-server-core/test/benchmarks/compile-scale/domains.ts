/**
 * 10 business domains x 10 entities = 100 distinct base cubes. Each entity names the entities it
 * references (many_to_one); those become foreign-key dimensions and joins. The first entity of a
 * domain is its fact table and carries the domain's view and rollup.
 */
export type EntitySpec = {
  name: string;
  refs: string[];
};

export type DomainSpec = {
  name: string;
  entities: EntitySpec[];
};

export const DOMAINS: DomainSpec[] = [
  {
    name: 'ecommerce',
    entities: [
      { name: 'orders', refs: ['customers', 'carts', 'payments'] },
      { name: 'order_items', refs: ['orders', 'products'] },
      { name: 'customers', refs: [] },
      { name: 'products', refs: ['categories'] },
      { name: 'categories', refs: [] },
      { name: 'carts', refs: ['customers'] },
      { name: 'payments', refs: ['customers'] },
      { name: 'shipments', refs: ['orders'] },
      { name: 'returns', refs: ['orders', 'products'] },
      { name: 'reviews', refs: ['customers', 'products'] },
    ],
  },
  {
    name: 'marketing',
    entities: [
      { name: 'conversions', refs: ['clicks', 'campaigns', 'leads'] },
      { name: 'campaigns', refs: ['channels', 'audiences'] },
      { name: 'ad_groups', refs: ['campaigns'] },
      { name: 'ads', refs: ['ad_groups'] },
      { name: 'impressions', refs: ['ads'] },
      { name: 'clicks', refs: ['ads', 'impressions'] },
      { name: 'email_sends', refs: ['campaigns', 'leads'] },
      { name: 'leads', refs: ['channels'] },
      { name: 'audiences', refs: [] },
      { name: 'channels', refs: [] },
    ],
  },
  {
    name: 'finance',
    entities: [
      { name: 'ledger_entries', refs: ['accounts', 'invoices', 'currencies'] },
      { name: 'invoices', refs: ['accounts', 'currencies'] },
      { name: 'invoice_lines', refs: ['invoices'] },
      { name: 'accounts', refs: [] },
      { name: 'budgets', refs: ['accounts'] },
      { name: 'expenses', refs: ['accounts', 'vendors'] },
      { name: 'vendors', refs: [] },
      { name: 'payouts', refs: ['vendors', 'currencies'] },
      { name: 'refunds', refs: ['invoices'] },
      { name: 'currencies', refs: [] },
    ],
  },
  {
    name: 'saas',
    entities: [
      { name: 'usage_records', refs: ['subscriptions', 'features', 'users'] },
      { name: 'subscriptions', refs: ['plans', 'organizations'] },
      { name: 'plans', refs: [] },
      { name: 'users', refs: ['organizations'] },
      { name: 'organizations', refs: [] },
      { name: 'sessions', refs: ['users'] },
      { name: 'events', refs: ['sessions', 'users'] },
      { name: 'features', refs: ['plans'] },
      { name: 'seats', refs: ['subscriptions', 'users'] },
      { name: 'trials', refs: ['organizations', 'plans'] },
    ],
  },
  {
    name: 'support',
    entities: [
      { name: 'tickets', refs: ['agents', 'queues', 'sla_policies'] },
      { name: 'ticket_comments', refs: ['tickets', 'agents'] },
      { name: 'agents', refs: ['queues'] },
      { name: 'sla_policies', refs: [] },
      { name: 'satisfaction_surveys', refs: ['tickets'] },
      { name: 'knowledge_articles', refs: ['agents'] },
      { name: 'chats', refs: ['agents', 'queues'] },
      { name: 'calls', refs: ['agents', 'queues'] },
      { name: 'escalations', refs: ['tickets', 'agents'] },
      { name: 'queues', refs: [] },
    ],
  },
  {
    name: 'hr',
    entities: [
      { name: 'payroll_runs', refs: ['employees', 'departments', 'positions'] },
      { name: 'employees', refs: ['departments', 'positions'] },
      { name: 'departments', refs: [] },
      { name: 'positions', refs: ['departments'] },
      { name: 'time_off', refs: ['employees'] },
      { name: 'performance_reviews', refs: ['employees'] },
      { name: 'candidates', refs: ['positions'] },
      { name: 'interviews', refs: ['candidates', 'employees'] },
      { name: 'offers', refs: ['candidates', 'positions'] },
      { name: 'trainings', refs: ['employees'] },
    ],
  },
  {
    name: 'logistics',
    entities: [
      { name: 'stock_movements', refs: ['inventory', 'warehouses', 'purchase_orders'] },
      { name: 'warehouses', refs: [] },
      { name: 'inventory', refs: ['warehouses', 'suppliers'] },
      { name: 'suppliers', refs: [] },
      { name: 'purchase_orders', refs: ['suppliers', 'warehouses'] },
      { name: 'po_lines', refs: ['purchase_orders', 'inventory'] },
      { name: 'carriers', refs: [] },
      { name: 'routes', refs: ['carriers', 'warehouses'] },
      { name: 'deliveries', refs: ['routes', 'vehicles'] },
      { name: 'vehicles', refs: ['carriers'] },
    ],
  },
  {
    name: 'manufacturing',
    entities: [
      { name: 'production_runs', refs: ['work_orders', 'machines', 'plants'] },
      { name: 'plants', refs: [] },
      { name: 'work_orders', refs: ['plants', 'materials'] },
      { name: 'machines', refs: ['plants'] },
      { name: 'bom_items', refs: ['materials', 'work_orders'] },
      { name: 'quality_checks', refs: ['production_runs', 'machines'] },
      { name: 'downtime_events', refs: ['machines'] },
      { name: 'materials', refs: [] },
      { name: 'defects', refs: ['quality_checks', 'materials'] },
      { name: 'maintenance', refs: ['machines', 'plants'] },
    ],
  },
  {
    name: 'iot',
    entities: [
      { name: 'readings', refs: ['sensors', 'devices', 'telemetry_batches'] },
      { name: 'devices', refs: ['sites', 'firmware'] },
      { name: 'alerts', refs: ['devices', 'sensors'] },
      { name: 'firmware', refs: [] },
      { name: 'sites', refs: [] },
      { name: 'gateways', refs: ['sites'] },
      { name: 'sensors', refs: ['devices'] },
      { name: 'anomalies', refs: ['sensors', 'readings'] },
      { name: 'commands', refs: ['devices', 'gateways'] },
      { name: 'telemetry_batches', refs: ['gateways'] },
    ],
  },
  {
    name: 'media',
    entities: [
      { name: 'page_views', refs: ['articles', 'readers', 'tags'] },
      { name: 'articles', refs: ['authors', 'tags'] },
      { name: 'authors', refs: [] },
      { name: 'readers', refs: [] },
      { name: 'videos', refs: ['authors'] },
      { name: 'video_plays', refs: ['videos', 'readers'] },
      { name: 'podcasts', refs: ['authors'] },
      { name: 'comments', refs: ['articles', 'readers'] },
      { name: 'newsletters', refs: ['authors'] },
      { name: 'tags', refs: [] },
    ],
  },
];

// Every join the generator writes needs a target inside the same domain
for (const domain of DOMAINS) {
  const names = new Set(domain.entities.map((e) => e.name));

  for (const entity of domain.entities) {
    const dangling = entity.refs.filter((r) => !names.has(r));
    if (dangling.length) {
      throw new Error(`${domain.name}.${entity.name} references unknown entities: ${dangling.join(', ')}`);
    }
  }
}

/**
 * Attribute vocabulary. Each cube takes a window of each pool starting at an entity-specific
 * offset, so cubes differ in their columns while every column still reads like a real one.
 */
export const STRING_ATTRS = [
  'status', 'type', 'channel', 'source', 'region', 'country', 'state', 'city', 'postal_code',
  'segment', 'tier', 'category', 'subcategory', 'brand', 'platform', 'device_type', 'browser',
  'os', 'language', 'currency_code', 'payment_method', 'priority', 'severity', 'owner_email',
  'external_ref', 'code', 'label', 'description', 'reason', 'notes', 'utm_source', 'utm_medium',
  'utm_campaign', 'team', 'cost_center', 'shift', 'grade', 'vendor_sku', 'model', 'version',
];

export const NUMBER_ATTRS = [
  'amount', 'quantity', 'unit_price', 'discount', 'tax', 'shipping_cost', 'cost', 'margin',
  'duration_seconds', 'weight_kg', 'distance_km', 'score', 'rating', 'retries', 'latency_ms',
  'balance', 'credit_limit', 'headcount', 'capacity', 'temperature', 'pressure', 'voltage',
  'impressions_count', 'clicks_count', 'bytes',
];

export const TIME_ATTRS = [
  'created_at', 'updated_at', 'completed_at', 'cancelled_at', 'scheduled_at', 'started_at',
  'ended_at', 'approved_at', 'due_at', 'closed_at',
];

export const BOOLEAN_ATTRS = [
  'is_active', 'is_deleted', 'is_test', 'is_internal', 'is_paid', 'is_refunded', 'is_verified',
  'is_flagged', 'is_recurring', 'is_premium',
];
