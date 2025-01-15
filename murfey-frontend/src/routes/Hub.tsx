import {
  Card,
  CardBody,
  CardHeader,
  Image,
  Link,
  Text,
  HStack,
  Heading,
  VStack,
  Box,
  SimpleGrid,
} from "@chakra-ui/react";

import { TbMicroscope, TbSnowflake } from "react-icons/tb";
import { Link as LinkRouter, useLoaderData } from "react-router-dom";
import { getInstrumentName } from "loaders/general";

import React from "react";

const getUrl = (endpoint: string) => {
  return process.env.REACT_APP_HUB_ENDPOINT + endpoint;
};

type InstrumentInfo = {
  instrument_name: string;
  display_name: string;
  instrument_url: string;
};

const Hub = () => {
  const instrumentInfo = useLoaderData() as InstrumentInfo[];

  const sessionStorageSetup = (ininfo: InstrumentInfo) => {
    sessionStorage.setItem("murfeyServerURL", ininfo.instrument_url + "/");
    sessionStorage.setItem("instrumentName", ininfo.instrument_name);
  };

  return (
    <Box w="100%" overflow="hidden">
      <VStack
        justifyContent="start"
        alignItems="start"
        display="flex"
        w="100%"
        px="10vw"
        py="1vh"
        bg="murfey.700"
      >
        <Heading size="xl" w="100%" color="murfey.50">
          <HStack>
            {" "}
            <TbSnowflake /> <TbMicroscope />{" "}
          </HStack>{" "}
          Murfey Hub
        </Heading>
      </VStack>
      <SimpleGrid
        minChildWidth="250px"
        spacing={10}
        p={3}
        justifyContent="start"
        alignItems="start"
        display="flex"
        w="100%"
      >
        {instrumentInfo ? (
          instrumentInfo.map((ini) => {
            return (
              <Link
                w={{ base: "100%", md: "19.6%" }}
                _hover={{ textDecor: "none" }}
                as={LinkRouter}
                to={`/login`}
              >
                <Card align="center" onClick={() => sessionStorageSetup(ini)}>
                  <CardHeader>
                    <Image
                      src={getUrl(`instrument/${ini.instrument_name}/image`)}
                    />
                  </CardHeader>
                  <CardBody>
                    <Text>{ini.display_name}</Text>
                  </CardBody>
                </Card>
              </Link>
            );
          })
        ) : (
          <></>
        )}
      </SimpleGrid>
    </Box>
  );
};

export { Hub };
