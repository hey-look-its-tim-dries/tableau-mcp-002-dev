import { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { ZodiosError } from '@zodios/core';
import { Err, Ok } from 'ts-results-es';
import { z } from 'zod';

import { getConfig } from '../../config.js';
import { useRestApi } from '../../restApiInstance.js';
import {
  Datasource,
  Query,
  QueryOutput,
  TableauError,
} from '../../sdks/tableau/apis/vizqlDataServiceApi.js';
import { Server } from '../../server.js';
import { getTableauAuthInfo } from '../../server/oauth/getTableauAuthInfo.js';
import { getVizqlDataServiceDisabledError } from '../getVizqlDataServiceDisabledError.js';
import { resourceAccessChecker } from '../resourceAccessChecker.js';
import { Tool } from '../tool.js';
import { getDatasourceCredentials } from './datasourceCredentials.js';
import { handleQueryDatasourceError } from './queryDatasourceErrorHandler.js';
import { validateQuery } from './queryDatasourceValidator.js';
import { queryDatasourceToolDescription } from './queryDescription.js';
import { validateFilterValues } from './validators/validateFilterValues.js';

type Datasource = z.infer<typeof Datasource>;

const paramsSchema = {
  datasourceLuid: z.string().nonempty(),
  query: Query,
};

export type QueryDatasourceError =
  | {
      type: 'feature-disabled';
    }
  | {
      type: 'datasource-not-allowed';
      message: string;
    }
  | {
      type: 'filter-validation';
      message: string;
    }
  | {
      type: 'tableau-error';
      error: z.infer<typeof TableauError>;
    };

export const getQueryDatasourceTool = (server: Server): Tool<typeof paramsSchema> => {
  const queryDatasourceTool = new Tool({
    server,
    name: 'Tableau-Answer',
    description: queryDatasourceToolDescription,
    paramsSchema,
    annotations: {
      title: 'Tableau answer',
      readOnlyHint: true,
      openWorldHint: false,
    },
    argsValidator: validateQuery,
    callback: async (
      { datasourceLuid, query },
      { requestId, authInfo },
    ): Promise<CallToolResult> => {
      const config = getConfig();
      return await queryDatasourceTool.logAndExecute<QueryOutput, QueryDatasourceError>({
        requestId,
        authInfo,
        args: { datasourceLuid, query },
        callback: async () => {
          const isDatasourceAllowedResult = await resourceAccessChecker.isDatasourceAllowed({
            datasourceLuid,
            restApiArgs: { config, requestId, server },
          });

          if (!isDatasourceAllowedResult.allowed) {
            return new Err({
              type: 'datasource-not-allowed',
              message: isDatasourceAllowedResult.message,
            });
          }

          const datasource: Datasource = { datasourceLuid };
          const options = {
            returnFormat: 'OBJECTS',
            debug: true,
            disaggregate: false,
          } as const;

          const credentials = getDatasourceCredentials(datasourceLuid);
          if (credentials) {
            datasource.connections = credentials;
          }

          const queryRequest = {
            datasource,
            query,
            options,
          };

          return await useRestApi({
            config,
            requestId,
            server,
            jwtScopes: ['tableau:viz_data_service:read'],
            authInfo: getTableauAuthInfo(authInfo),
            callback: async (restApi) => {
              if (!config.disableQueryDatasourceFilterValidation) {
                // Validate filters values for SET and MATCH filters
                const filterValidationResult = await validateFilterValues(
                  server,
                  query,
                  restApi.vizqlDataServiceMethods,
                  datasource,
                );

                if (filterValidationResult.isErr()) {
                  const errors = filterValidationResult.error;
                  const errorMessage = errors.map((error) => error.message).join(', ');
                  return new Err({
                    type: 'filter-validation',
                    message: errorMessage,
                  });
                }
              }

              const result = await restApi.vizqlDataServiceMethods.queryDatasource(queryRequest);
              if (result.isErr()) {
                return new Err(
                  result.error instanceof ZodiosError
                    ? result.error
                    : result.error === 'feature-disabled'
                      ? { type: 'feature-disabled' }
                      : {
                          type: 'tableau-error',
                          error: result.error,
                        },
                );
              }
              return result;
            },
          });
        },
        constrainSuccessResult: (queryOutput) => {
          return {
            type: 'success',
            result: queryOutput,
          };
        },
        getErrorText: (error: QueryDatasourceError) => {
          switch (error.type) {
            case 'feature-disabled':
              return getVizqlDataServiceDisabledError();
            case 'datasource-not-allowed':
              return error.message;
            case 'filter-validation':
              return JSON.stringify({
                requestId,
                errorType: 'validation',
                message: error.message,
              });
            case 'tableau-error':
              return JSON.stringify({
                requestId,
                ...handleQueryDatasourceError(error.error),
              });
          }
        },
      });
    },
  });

  return queryDatasourceTool;
};

// -------------------------------
// Wrapper tool (flat schema) for LLM-friendly calling
// -------------------------------

const simpleParamsSchema = {
  datasourceName: z.string().min(1),
  question: z.string().min(1),
  limit: z.number().int().min(1).max(200).optional(),
};

type SimpleQueryDatasourceError =
  | {
      type: 'datasource-not-found';
      message: string;
    }
  | {
      type: 'datasource-not-allowed';
      message: string;
    }
  | {
      type: 'feature-disabled';
    }
  | {
      type: 'tableau-error';
      error: z.infer<typeof TableauError>;
    };

/**
 * LLM-friendly wrapper around the complex `Tableau-Answer` tool.
 * Accepts only a datasourceName + question, resolves the datasource LUID,
 * constructs a conservative query (COUNT by default), and delegates to VizQL Data Service.
 */
export const getSimpleQueryDatasourceTool = (server: Server): Tool<typeof simpleParamsSchema> => {
  const simpleTool = new Tool({
    server,
    name: 'simple_query_datasource',
    description:
      'Answer a question by querying a Tableau published datasource. Provide datasourceName and question.',
    paramsSchema: simpleParamsSchema,
    annotations: {
      title: 'Simple Tableau query',
      readOnlyHint: true,
      openWorldHint: false,
    },
    callback: async (
      { datasourceName, question, limit },
      { requestId, authInfo },
    ): Promise<CallToolResult> => {
      const config = getConfig();
      const rowLimit = limit ?? 20;

      return await simpleTool.logAndExecute<QueryOutput, SimpleQueryDatasourceError>({
        requestId,
        authInfo,
        args: { datasourceName, question, limit },
        callback: async () => {
          // 1) Resolve datasourceName -> datasourceLuid by calling Tableau REST listDatasources
          const resolved = await useRestApi({
            config,
            requestId,
            server,
            jwtScopes: ['tableau:content:read'],
            authInfo: getTableauAuthInfo(authInfo),
            callback: async (restApi) => {
              const target = datasourceName.trim().toLowerCase();

              // Try a few pages (keeps it predictable; increase if needed)
              const pageSize = 100;
              for (let pageNumber = 1; pageNumber <= 5; pageNumber++) {
                const { datasources } = await restApi.datasourcesMethods.listDatasources({
                  siteId: restApi.siteId,
                  filter: '',
                  pageSize,
                  pageNumber,
                });

                const match = bestDatasourceMatch(datasources ?? [], target);
                if (match) {
                  return Ok(match);
                }

                // If we got fewer than pageSize, we're likely at the end.
                if (!datasources || datasources.length < pageSize) {
                  break;
                }
              }

              return Err({
                type: 'datasource-not-found',
                message: `No datasource matched "${datasourceName}".`,
              } as SimpleQueryDatasourceError);
            },
          });

          if (resolved.isErr()) {
            return resolved;
          }

          const datasourceLuid = (resolved.value as any).luid ?? (resolved.value as any).id;
          if (!datasourceLuid) {
            return Err({
              type: 'datasource-not-found',
              message: `Datasource "${datasourceName}" was found but had no LUID/ID in the response.`,
            });
          }

          // 2) Enforce your existing datasource allow-list logic
          const isDatasourceAllowedResult = await resourceAccessChecker.isDatasourceAllowed({
            datasourceLuid,
            restApiArgs: { config, requestId, server },
          });

          if (!isDatasourceAllowedResult.allowed) {
            return Err({
              type: 'datasource-not-allowed',
              message: isDatasourceAllowedResult.message,
            });
          }

          // 3) Read metadata (VizQL Data Service) so we can pick a real field caption
          const readMetadataResult = await useRestApi({
            config,
            requestId,
            server,
            jwtScopes: ['tableau:viz_data_service:read'],
            authInfo: getTableauAuthInfo(authInfo),
            callback: async (restApi) => {
              const md = await restApi.vizqlDataServiceMethods.readMetadata({
                datasource: { datasourceLuid },
              });

              if (md.isErr()) {
                return Err({ type: 'feature-disabled' } as SimpleQueryDatasourceError);
              }

              return Ok(md.value);
            },
          });

          if (readMetadataResult.isErr()) {
            return readMetadataResult;
          }

          const firstFieldCaption = extractFirstFieldCaption(readMetadataResult.value);

          // 4) Build a conservative query (COUNT by default)
          const query: z.infer<typeof Query> = buildQueryFromQuestion({
            question,
            firstFieldCaption,
            rowLimit,
          });

          // 5) Delegate to VizQL queryDatasource using the same approach as `Tableau-Answer`
          const datasource: z.infer<typeof Datasource> = { datasourceLuid };
          const options = {
            returnFormat: 'OBJECTS',
            debug: false,
            disaggregate: false,
          } as const;

          const credentials = getDatasourceCredentials(datasourceLuid);
          if (credentials) {
            (datasource as any).connections = credentials;
          }

          const queryRequest = {
            datasource,
            query,
            options,
          };

          return await useRestApi({
            config,
            requestId,
            server,
            jwtScopes: ['tableau:viz_data_service:read'],
            authInfo: getTableauAuthInfo(authInfo),
            callback: async (restApi) => {
              if (!config.disableQueryDatasourceFilterValidation) {
                const filterValidationResult = await validateFilterValues(
                  server,
                  query,
                  restApi.vizqlDataServiceMethods,
                  datasource as any,
                );

                if (filterValidationResult.isErr()) {
                  const errors = filterValidationResult.error;
                  const errorMessage = errors.map((error) => error.message).join(', ');
                  return Err({
                    type: 'tableau-error',
                    error: {
                      errorCode: 'validation',
                      message: errorMessage,
                    } as any,
                  } as SimpleQueryDatasourceError);
                }
              }

              const result = await restApi.vizqlDataServiceMethods.queryDatasource(queryRequest);
              if (result.isErr()) {
                return Err(
                  result.error instanceof ZodiosError
                    ? ({
                        type: 'tableau-error',
                        error: result.error as any,
                      } as SimpleQueryDatasourceError)
                    : (result.error === 'feature-disabled'
                        ? ({ type: 'feature-disabled' } as SimpleQueryDatasourceError)
                        : ({
                            type: 'tableau-error',
                            error: result.error,
                          } as SimpleQueryDatasourceError)),
                );
              }

              return result;
            },
          });
        },
        constrainSuccessResult: (queryOutput) => {
          return {
            type: 'success',
            result: queryOutput,
          };
        },
        getErrorText: (error: SimpleQueryDatasourceError) => {
          switch (error.type) {
            case 'feature-disabled':
              return getVizqlDataServiceDisabledError();
            case 'datasource-not-found':
            case 'datasource-not-allowed':
              return error.message;
            case 'tableau-error':
              return JSON.stringify({
                requestId,
                ...handleQueryDatasourceError(error.error),
              });
          }
        },
      });
    },
  });

  return simpleTool;
};

function bestDatasourceMatch(list: any[], targetLower: string) {
  // Prefer exact match, then contains match
  const exact = list.find((d) => (d.name ?? '').toLowerCase() === targetLower);
  if (exact) return exact;
  return list.find((d) => (d.name ?? '').toLowerCase().includes(targetLower));
}

function extractFirstFieldCaption(readMetadataValue: any): string {
  // Try common shapes in VizQL metadata responses.
  const candidates =
    readMetadataValue?.fields ??
    readMetadataValue?.result?.fields ??
    readMetadataValue?.data?.fields ??
    readMetadataValue?.metadata?.fields ??
    [];

  const first = Array.isArray(candidates) ? candidates[0] : undefined;
  return (
    first?.fieldCaption ??
    first?.name ??
    first?.caption ??
    // Fallback (may fail if datasource doesn't have this caption)
    'Number of Records'
  );
}

function buildQueryFromQuestion(args: {
  question: string;
  firstFieldCaption: string;
  rowLimit: number;
}): z.infer<typeof Query> {
  // Conservative default: COUNT of a real field.
  // You can iterate later to support GROUP BY / filters / time series based on question.
  return {
    fields: [
      {
        fieldCaption: args.firstFieldCaption,
        function: 'COUNT',
        fieldAlias: 'Total Records',
      },
    ],
    limit: args.rowLimit,
  } as any;
}
