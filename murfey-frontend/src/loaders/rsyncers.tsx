import { QueryClient } from "@tanstack/react-query";
import { components } from "schema/main";
import { client } from "utils/api/client";
import { Params } from "react-router-dom";
import { parseDate } from "utils/generic";

const getRsyncerData = async (sessionId: string) => {
  const response = await client.get(`sessions/${sessionId}/rsyncers`);

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

export const pauseRsyncer = async (sessionId: number, source: string) => {
  console.log("stopping rsyncer");

  const response = await client.post(`sessions/${sessionId}/stop_rsyncer`, {
    source: source,
  });

  if (response.status !== 200) {
    return null;
  }

  console.log(response.data);

  return response.data;
};

export const restartRsyncer = async (sessionId: number, source: string) => {
  console.log("stopping rsyncer");

  const response = await client.post(`sessions/${sessionId}/restart_rsyncer`, {
    source: source,
  });

  if (response.status !== 200) {
    return null;
  }

  console.log(response.data);

  return response.data;
};

export const finaliseRsyncer = async (sessionId: number, source: string) => {
  const response = await client.post(`sessions/${sessionId}/finalise_rsyncer`, {
    source: source,
  });

  if (response.status !== 200) {
    return null;
  }

  console.log(response.data);

  return response.data;
};

export const removeRsyncer = async (sessionId: number, source: string) => {
  console.log("removing rsyncer");

  const response = await client.post(`sessions/${sessionId}/remove_rsyncer`, {
    source: source,
  });

  if (response.status !== 200) {
    return null;
  }

  console.log(response.data);

  return response.data;
};

const queryBuilder = (sessionId: string = "0") => {
  return {
    queryKey: ["sessid", sessionId],
    queryFn: () => getRsyncerData(sessionId),
    staleTime: 60000,
  };
};

export const rsyncerLoader =
  (queryClient: QueryClient) => async (params: Params) => {
    const singleQuery = queryBuilder(params.sessid);
    return (
      (await queryClient.getQueryData(singleQuery.queryKey)) ??
      (await queryClient.fetchQuery(singleQuery))
    );
  };
