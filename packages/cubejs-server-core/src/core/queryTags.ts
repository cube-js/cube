import type {
  DriverContext, LoggerFn, QueryTags, QueryTagsFn, QueryTagsInternalFn, QueryTagValue,
} from './types';

/**
 * Keys follow the strictest data source rules (BigQuery labels), so drivers can
 * attach the tags as they are. Values stay free-form, as their encoding differs by data source.
 */
const QUERY_TAG_KEY = /^[a-z][a-z0-9_-]{0,62}$/;

/** BigQuery allows 64 labels per job, and one of them is `cube_request_id`. */
export const MAX_QUERY_TAGS = 63;

/**
 * Wraps the `queryTags` option once, so every driver receives tags that are already validated.
 * Tags only attribute cost, so neither a throwing hook nor an invalid tag keeps users from their data.
 */
export function wrapQueryTagsFn(queryTagsFn: QueryTagsFn, logger: LoggerFn): QueryTagsInternalFn {
  return async (context: DriverContext) => {
    const { requestId } = context;

    let rawTags: Record<string, QueryTagValue> | undefined;

    try {
      rawTags = await queryTagsFn(context);
    } catch (e) {
      logger('Query Tags Error', { requestId, error: (e as Error).stack || String(e) });
      return undefined;
    }

    const queryTags: QueryTags = {};
    // A missing value, e.g. an optional security context field, is skipped without a log
    const entries = Object.entries(rawTags || {}).filter(([, value]) => value != null);

    for (const [key, value] of entries) {
      if (!QUERY_TAG_KEY.test(key)) {
        logger('Query Tag Dropped', { key, reason: 'invalid_key', requestId });
      } else if (Object.keys(queryTags).length >= MAX_QUERY_TAGS) {
        logger('Query Tag Dropped', { key, reason: 'tag_limit', requestId });
      } else {
        queryTags[key] = String(value);
      }
    }

    return Object.keys(queryTags).length ? queryTags : undefined;
  };
}
