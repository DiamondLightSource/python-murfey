import { QueryClient } from "@tanstack/react-query";
import { components } from "schema/main";
import { client } from "utils/api/client";
import { Params } from "react-router-dom";

export const includePage = (endpoint: string, limit: number, page: number) =>
  `${endpoint}${endpoint.includes("?") ? "&" : "?"}page=${
    page - 1
  }&limit=${limit}`;

const getSessionsData = async () => {
  const response = await client.get(`sessions`);

  if (response.status !== 200) {
    return null;
  }

  return {
    current: response.data,
  };
};

const getClientData = async () => {
  const response = await client.get(`clients`);

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

const getSessionData = async (sessid: string = "0") => {
  const response = await client.get(`session/${sessid}`);

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

export const linkSessionToClient = async (
  client_id: number,
  sessionName: string,
) => {
  const response = await client.post(`clients/${client_id}/session`, {
    session_name: sessionName,
  });
  if (response.status !== 200) {
    return null;
  }
  return response.data;
};

export const createSession = async (
  visit: string,
  sessionName: string,
  instrumentName: string,
) => {
  const response = await client.post(
    `instruments/${instrumentName}/visits/${visit}/session/${sessionName}`,
    {},
  );
  if (response.status !== 200) {
    return null;
  }
  return response.data;
};

export const deleteSessionData = async (sessid: number) => {
  const response = await client.delete(`sessions/${sessid}`);
  if (response.status !== 200) {
    return null;
  }
  return response.data;
};

const query = {
  queryKey: ["homepageSessions", sessionStorage.getItem("instrumentName")],
  queryFn: getSessionsData,
  staleTime: 60000,
};

const clientQuery = {
  queryKey: ["homepageClients", sessionStorage.getItem("instrumentName")],
  queryFn: getClientData,
  staleTime: 60000,
};

export const sessionsLoader = (queryClient: QueryClient) => async () =>
  (await queryClient.getQueryData(query.queryKey)) ??
  (await queryClient.fetchQuery(query));

export const clientsLoader = (queryClient: QueryClient) => async () =>
  (await queryClient.getQueryData(clientQuery.queryKey)) ??
  (await queryClient.fetchQuery(clientQuery));

const queryBuilder = (sessid: string = "0") => {
  return {
    queryKey: ["sessionId", sessid],
    queryFn: () => getSessionData(sessid),
    staleTime: 60000,
  };
};

export const sessionLoader =
  (queryClient: QueryClient) => async (params: Params) => {
    const singleQuery = queryBuilder(params.sessid);
    return (
      (await queryClient.getQueryData(singleQuery.queryKey)) ??
      (await queryClient.fetchQuery(singleQuery))
    );
  };

export { getSessionData };
