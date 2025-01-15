import {
  Accordion,
  AccordionButton,
  AccordionPanel,
  AccordionItem,
  AccordionIcon,
  Box,
  Heading,
  IconButton,
  HStack,
  VStack,
  Switch,
  Text,
} from "@chakra-ui/react";

import { useLoaderData } from "react-router-dom";
import { components } from "schema/main";
import { Table } from "@diamondlightsource/ui-components";
import { MdEditNote } from "react-icons/md";

import React from "react";

type ProcessingDetails = components["schemas"]["ProcessingDetails"];

type ProcessingRow = {
  parameterName: string;
  parameterValue?: string | number | boolean;
};

type ProcessingTable = {
  processingRows: ProcessingRow[];
  tag: string;
};

const nameLabelMap: Map<string, string> = new Map([
  ["pj_id", "Processing Job ID"],
  ["angpix", "Pixel Size [m]"],
  ["dose_per_frame", "Dose per frame [\u212B / pixel]"],
  ["gain_ref", "Gain Reference"],
  ["voltage", "Voltage [kV]"],
  ["motion_corr_binning", "Motion correction binning factor"],
  ["eer_grouping", "EER Grouping"],
  ["symmetry", "Symmetry"],
  ["particle_diameter", "Particle Diameter [\u212B]"],
  ["downscale", "Downscaling During Extraction"],
  ["do_icebreaker_jobs", "Perform IceBreaker Jobs"],
  ["boxsize", "Box Size"],
  ["small_boxsize", "Downscaled Box Size"],
  ["mask_diameter", "Mask Diameter for Classification"],
  ["estimate_particle_diameter", "Automatically Estimate Particle Diameter"],
  ["hold_class2d", "2D Classification Held"],
  ["rerun_class2d", "First 2D Classification Batch Needs to be Rerun"],
  ["rerun_class3d", "3D Classification Needs to be Rerun"],
  ["class_selection_score", "Class Selection Threshold"],
  ["star_combination_job", "Job Number for Rebatching Job"],
  ["initial_model", "Initial Model"],
  ["next_job", "Next Job Number"],
  ["picker_murfey_id", "Murfey ID of Picker for Use in ISPyB"],
  ["picker_ispyb_id", "ISPyB Particle Picker ID"],
]);

const ProcessingParameters = () => {
  const procParams = useLoaderData() as ProcessingDetails[] | null;
  const [showExtra, setShowExtra] = React.useState(false);
  let tableRows = [] as ProcessingTable[];
  let tableRowsExtra = [] as ProcessingTable[];
  console.log(procParams);
  procParams?.map((p) => {
    let tr: ProcessingTable = {
      processingRows: [],
      tag: "",
    };
    let tre: ProcessingTable = {
      processingRows: [],
      tag: "",
    };
    Object.entries(p?.relion_params ? p?.relion_params : {}).forEach(
      ([key, value]) =>
        tr.processingRows.push({
          parameterName: nameLabelMap.get(key) ?? key,
          parameterValue:
            value === true ? "True" : value === false ? "False" : value,
        }),
    );
    tr.tag = p?.relion_params.pj_id.toString();
    tableRows.push(tr);
    Object.entries(p?.feedback_params ? p?.feedback_params : {}).forEach(
      ([key, value]) =>
        tre.processingRows.push({
          parameterName: nameLabelMap.get(key) ?? key,
          parameterValue:
            value === true ? "True" : value === false ? "False" : value,
        }),
    );
    tre.tag = p?.feedback_params.pj_id.toString();
    tableRowsExtra.push(tre);
  });
  const handleToggle = (event: React.ChangeEvent<HTMLInputElement>) => {
    setShowExtra(!showExtra);
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
                Processing Parameters
              </Heading>
              <HStack>
                <Switch colorScheme="murfey" onChange={handleToggle} />
                <Text color="murfey.50">Show extra processing parameters</Text>
              </HStack>
            </VStack>
          </VStack>
        </Box>
        <Accordion>
          {tableRows.map((tr) => {
            return (
              <AccordionItem>
                <AccordionButton bg="murfey.400">
                  <Box as="span" flex="1" textAlign="left">
                    Main Processing Parameters (Processing Job ID {tr.tag})
                  </Box>
                  <AccordionIcon />
                </AccordionButton>
                <AccordionPanel>
                  <Table
                    data={tr.processingRows}
                    headers={[
                      { key: "parameterName", label: "Parameter" },
                      { key: "parameterValue", label: "Value" },
                    ]}
                    label={"processingParameterData"}
                  />
                </AccordionPanel>
              </AccordionItem>
            );
          })}
          {showExtra ? (
            tableRowsExtra.map((tre) => {
              return (
                <AccordionItem>
                  <AccordionButton bg="murfey.500">
                    <Box as="span" flex="1" textAlign="left">
                      Extra Processing Parameters (Processing Job ID {tre.tag})
                    </Box>
                    <AccordionIcon />
                  </AccordionButton>
                  <AccordionPanel>
                    <IconButton
                      aria-label="Edit parameters"
                      icon={<MdEditNote />}
                    />
                    <Table
                      data={tre.processingRows}
                      headers={[
                        { key: "parameterName", label: "Parameter" },
                        { key: "parameterValue", label: "Value" },
                      ]}
                      label={"processingParameterData"}
                    />
                  </AccordionPanel>
                </AccordionItem>
              );
            })
          ) : (
            <></>
          )}
        </Accordion>
      </Box>
    </div>
  );
};

export { ProcessingParameters };
