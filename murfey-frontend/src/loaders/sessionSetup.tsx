import { QueryClient } from "@tanstack/react-query";
import { client } from "utils/api/client";
import { components } from "schema/main";

type ProvidedProcessingParameters =
  components["schemas"]["ProvidedProcessingParameters"];

export const registerProcessingParameters = async (
  processingParameters: ProvidedProcessingParameters,
  sessionId: number,
) => {
  const response = await client.post(
    `sessions/${sessionId}/provided_processing_parameters`,
    processingParameters,
  );

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};
