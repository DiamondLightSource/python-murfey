import { QueryClient } from "@tanstack/react-query";
import { client } from "utils/api/client";
import { Params } from "react-router-dom";

const getProcessingParameterData = async (sessid: string = "0") => {
  const response = await client.get(
    `sessions/${sessid}/spa_processing_parameters`,
  );

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

const queryBuilder = (sessid: string = "0") => {
  return {
    queryKey: ["sessionId", sessid],
    queryFn: () => getProcessingParameterData(sessid),
    staleTime: 60000,
  };
};

export const processingParametersLoader =
  (queryClient: QueryClient) => async (params: Params) => {
    const singleQuery = queryBuilder(params.sessid);
    return (
      (await queryClient.getQueryData(singleQuery.queryKey)) ??
      (await queryClient.fetchQuery(singleQuery))
    );
  };

export { getProcessingParameterData };
