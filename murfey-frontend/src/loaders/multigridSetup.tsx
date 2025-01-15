import { QueryClient } from "@tanstack/react-query";
import { client } from "utils/api/client";
import { components } from "schema/main";

type MultigridWatcherSpec = components["schemas"]["MultigridWatcherSetup"];

export const startMultigridWatcher = async (
  multigridWatcher: MultigridWatcherSpec,
  sessionId: number,
) => {
  const response = await client.post(
    `sessions/${sessionId}/multigrid_watcher`,
    multigridWatcher,
  );

  if (response.status !== 200) {
    return null;
  }

  return response.data;
};
