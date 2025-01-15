import { Box, Button, Heading, HStack, VStack, Link } from "@chakra-ui/react";

import {
  useNavigate,
  useLoaderData,
  useParams,
  Link as LinkRouter,
} from "react-router-dom";
import { components } from "schema/main";
import { Table } from "@diamondlightsource/ui-components";
import { SetupStepper } from "components/setupStepper";
import {
  prepareGainReference,
  transferGainReference,
  updateCurrentGainReference,
} from "loaders/possibleGainRefs";
import { getMachineConfigData } from "loaders/machineConfig";

import { useState } from "react";

type DataCollectionGroup = components["schemas"]["DataCollectionGroup"];

const DataCollectionGroups = () => {
  const dataCollectionGroups = useLoaderData() as {
    [key: string]: DataCollectionGroup;
  };
  const { sessid } = useParams();
  const navigate = useNavigate();

  const SelectDataCollectionGroup = async (
    data: Record<string, any>,
    index: number,
  ) => {
    navigate(
      `/sessions/${sessid}/data_collection_groups/${data["id"]}/grid_squares`,
    );
  };

  return (
    <div className="rootContainer">
      <Box w="100%" bg="murfey.50">
        <Box w="100%" overflow="hidden">
          <VStack className="homeRoot">
            <VStack
              bg="murfey.700"
              justifyContent="start"
              alignItems="start"
              display="flex"
              w="100%"
              px="10vw"
              py="1vh"
            >
              <Heading size="xl" color="murfey.50">
                Data Collection Groups
              </Heading>
            </VStack>
          </VStack>
        </Box>
        <Box
          mt="1em"
          px="10vw"
          w="100%"
          justifyContent={"center"}
          alignItems={"center"}
        >
          {dataCollectionGroups ? (
            <Table
              width="80%"
              data={Object.values(dataCollectionGroups)}
              headers={[
                { key: "tag", label: "Tag" },
                { key: "id", label: "ID" },
                { key: "atlas", label: "Atlas" },
              ]}
              label={"dataCollectionGroupsData"}
              onClick={SelectDataCollectionGroup}
            />
          ) : (
            <></>
          )}
        </Box>
      </Box>
    </div>
  );
};

export { DataCollectionGroups };
