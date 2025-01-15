import { QueryClient } from "@tanstack/react-query";
import { client } from "utils/api/client";
import { Params } from "react-router-dom";

const getGainRefData = async (sessionId: string) => {
  const response = await client.get(
    `instruments/${sessionStorage.getItem(
      "instrumentName",
    )}/sessions/${sessionId}/possible_gain_references`,
  );

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

export const transferGainReference = async (
  sessionId: number,
  gainRef: string,
) => {
  const response = await client.post(
    `sessions/${sessionId}/upload_gain_reference`,
    {
      gain_path: gainRef,
    },
  );
  if (response.status !== 200) {
    return null;
  }
  return response.data;
};

export const prepareGainReference = async (
  sessionId: number,
  gainRef: string,
  rescale: boolean = false,
  eer: boolean = false,
  tag: string = "",
) => {
  const response = await client.post(`sessions/${sessionId}/process_gain`, {
    gain_ref: gainRef,
    rescale: rescale,
    eer: eer,
    tag: tag,
  });
  if (response.status !== 200) {
    return null;
  }
  return response.data;
};

export const updateCurrentGainReference = async (
  sessionId: number,
  gainRef: string,
) => {
  const response = await client.put(`sessions/${sessionId}/current_gain_ref`, {
    path: gainRef,
  });
  if (response.status !== 200) {
    return null;
  }
  return response.data;
};

const query = (sessid: string) => {
  return {
    queryKey: ["gainRefs"],
    queryFn: () => getGainRefData(sessid),
    staleTime: 60000,
  };
};

export const gainRefLoader =
  (queryClient: QueryClient) => async (params: Params) => {
    if (params.sessid) {
      const singleQuery = query(params.sessid);
      return (
        (await queryClient.getQueryData(singleQuery.queryKey)) ??
        (await queryClient.fetchQuery(singleQuery))
      );
    }
  };
