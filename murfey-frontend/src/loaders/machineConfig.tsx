import { QueryClient } from "@tanstack/react-query";
import { components } from "schema/main";
import { client } from "utils/api/client";
import { Params } from "react-router-dom";
import { parseDate } from "utils/generic";

export const getMachineConfigData = async () => {
  const response = await client.get(
    `instruments/${sessionStorage.getItem("instrumentName")}/machine`,
  );

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

const query = {
  queryKey: ["machineConfig"],
  queryFn: getMachineConfigData,
  staleTime: 60000,
};

export const machineConfigLoader = (queryClient: QueryClient) => async () =>
  (await queryClient.getQueryData(query.queryKey)) ??
  (await queryClient.fetchQuery(query));
