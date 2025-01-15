import { QueryClient } from "@tanstack/react-query";
import { client } from "utils/api/client";
import { Params } from "react-router-dom";

const getGridSquares = async (
  sessid: string = "0",
  dataCollectionGroupId: string = "0",
) => {
  console.log("getting grid squares");
  const response = await client.get(
    `sessions/${sessid}/data_collection_groups/${dataCollectionGroupId}/grid_squares`,
  );

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

const getNumMovies = async (
  sessid: string,
  dataCollectionGroupId: string,
  gridSquareId: number,
) => {
  const response = await client.get(
    `sessions/${sessid}/data_collection_groups/${dataCollectionGroupId}/grid_squares/${gridSquareId}/num_movies`,
  );

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

const getFoilHoles = async (
  sessid: string,
  dataCollectionGroupId: string,
  gridSquareId: number,
) => {
  const response = await client.get(
    `sessions/${sessid}/data_collection_groups/${dataCollectionGroupId}/grid_squares/${gridSquareId}/foil_holes`,
  );

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};

const queryBuilder = (
  sessid: string = "0",
  dataCollectionGroupId: string = "0",
) => {
  return {
    queryKey: [
      "gridSquares",
      "sessionId",
      sessid,
      "dataCollectionGroup",
      dataCollectionGroupId,
    ],
    queryFn: () => getGridSquares(sessid, dataCollectionGroupId),
    staleTime: 60000,
  };
};

export const gridSquaresLoader =
  (queryClient: QueryClient) => async (params: Params) => {
    // const singleQuery = queryBuilder(params.sessid, params.dcgid);
    const singleQuery = queryBuilder(params.sessid);
    return (
      (await queryClient.getQueryData(singleQuery.queryKey)) ??
      (await queryClient.fetchQuery(singleQuery))
    );
  };

export { getFoilHoles, getNumMovies };
