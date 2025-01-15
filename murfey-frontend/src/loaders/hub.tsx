import { QueryClient } from "@tanstack/react-query";
import { client, getPrefix } from "utils/api/client";

const getInstrumentInfo = async () => {
  const response = await client.hub_get(`instruments`);

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

const query = {
  queryKey: ["instrumentInfo"],
  queryFn: getInstrumentInfo,
  staleTime: 60000,
};

export const instrumentInfoLoader = (queryClient: QueryClient) => async () =>
  (await queryClient.getQueryData(query.queryKey)) ??
  (await queryClient.fetchQuery(query));
