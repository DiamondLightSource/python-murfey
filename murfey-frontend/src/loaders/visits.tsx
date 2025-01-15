import { QueryClient } from "@tanstack/react-query";
import { client } from "utils/api/client";
import { Params } from "react-router-dom";

const getVisitData = async (instrumentName: string) => {
  const response = await client.get(`instruments/${instrumentName}/visits_raw`);

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

const query = (instrumentName: string) => {
  return {
    queryKey: ["visits", instrumentName],
    queryFn: () => getVisitData(instrumentName),
    staleTime: 60000,
  };
};

export const visitLoader =
  (queryClient: QueryClient) => async (params: Params) => {
    if (params.instrumentName) {
      const singleQuery = query(params.instrumentName);
      return (
        (await queryClient.getQueryData(singleQuery.queryKey)) ??
        (await queryClient.fetchQuery(singleQuery))
      );
    }
    return null;
  };
