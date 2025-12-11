import { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { Ok } from 'ts-results-es';
import { z } from 'zod';

import { BoundedContext, getConfig } from '../../config.js';
import { useRestApi } from '../../restApiInstance.js';
import { DataSource } from '../../sdks/tableau/types/dataSource.js';
import { Server } from '../../server.js';
import { getTableauAuthInfo } from '../../server/oauth/getTableauAuthInfo.js';
import { paginate } from '../../utils/paginate.js';
import { genericFilterDescription } from '../genericFilterDescription.js';
import { ConstrainedResult, Tool } from '../tool.js';
import { parseAndValidateDatasourcesFilterString } from './datasourcesFilterUtils.js';

const paramsSchema = {
  filter: z.string().optional(),
  pageSize: z.number().gt(0).optional(),
  limit: z.number().gt(0).optional(),
};

export const getListDatasourcesTool = (server: Server): Tool<typeof paramsSchema> => {
  const listDatasourcesTool = new Tool({
    server,
    name: 'list-datasources',
    description: `
 
  `,
    paramsSchema,
    annotations: {
      title: 'List Datasources',
      readOnlyHint: true,
      openWorldHint: false,
    },
    callback: async (
      { filter, pageSize, limit },
      { requestId, authInfo },
    ): Promise<CallToolResult> => {
      const config = getConfig();
      const validatedFilter = filter ? parseAndValidateDatasourcesFilterString(filter) : undefined;
      return await listDatasourcesTool.logAndExecute({
        requestId,
        authInfo,
        args: { filter, pageSize, limit },
        callback: async () => {
          const datasources = await useRestApi({
            config,
            requestId,
            server,
            jwtScopes: ['tableau:content:read'],
            authInfo: getTableauAuthInfo(authInfo),
            callback: async (restApi) => {
              const datasources = await paginate({
                pageConfig: {
                  pageSize,
                  limit: config.maxResultLimit
                    ? Math.min(config.maxResultLimit, limit ?? Number.MAX_SAFE_INTEGER)
                    : limit,
                },
                getDataFn: async (pageConfig) => {
                  const { pagination, datasources: data } =
                    await restApi.datasourcesMethods.listDatasources({
                      siteId: restApi.siteId,
                      filter: validatedFilter ?? '',
                      pageSize: pageConfig.pageSize,
                      pageNumber: pageConfig.pageNumber,
                    });

                  return { pagination, data };
                },
              });

              return datasources;
            },
          });

          return new Ok(datasources);
        },
        constrainSuccessResult: (datasources) =>
          constrainDatasources({ datasources, boundedContext: config.boundedContext }),
      });
    },
  });

  return listDatasourcesTool;
};

export function constrainDatasources({
  datasources,
  boundedContext,
}: {
  datasources: Array<DataSource>;
  boundedContext: BoundedContext;
}): ConstrainedResult<Array<DataSource>> {
  if (datasources.length === 0) {
    return {
      type: 'empty',
      message:
        'No datasources were found. Either none exist or you do not have permission to view them.',
    };
  }

  const { projectIds, datasourceIds } = boundedContext;
  if (projectIds) {
    datasources = datasources.filter((datasource) => projectIds.has(datasource.project.id));
  }

  if (datasourceIds) {
    datasources = datasources.filter((datasource) => datasourceIds.has(datasource.id));
  }

  if (datasources.length === 0) {
    return {
      type: 'empty',
      message: [
        'The set of allowed data sources that can be queried is limited by the server configuration.',
        'While data sources were found, they were all filtered out by the server configuration.',
      ].join(' '),
    };
  }

  return {
    type: 'success',
    result: datasources,
  };
}
