import { QueryClient } from "@tanstack/react-query";
import { client } from "utils/api/client";
import { Params } from "react-router-dom";

const getDataCollectionGroups = async (sessid: string = "0") => {
  console.log("data collection groups gather");
  const response = await client.get(
    `sessions/${sessid}/data_collection_groups`,
  );

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

const queryBuilder = (sessid: string = "0") => {
  return {
    queryKey: ["sessionId", sessid],
    queryFn: () => getDataCollectionGroups(sessid),
    staleTime: 60000,
  };
};

export const dataCollectionGroupsLoader =
  (queryClient: QueryClient) => async (params: Params) => {
    const singleQuery = queryBuilder(params.sessid);
    return (
      (await queryClient.getQueryData(singleQuery.queryKey)) ??
      (await queryClient.fetchQuery(singleQuery))
    );
  };
