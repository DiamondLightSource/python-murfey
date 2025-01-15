import {
  Box,
  Heading,
  HStack,
  VStack,
  CardBody,
  Card,
  CardHeader,
  Image,
} from "@chakra-ui/react";

import { useNavigate, useLoaderData, useParams } from "react-router-dom";
import { components } from "schema/main";
import { GridSquareCard } from "components/gridSquareCard";

type GridSquare = components["schemas"]["GridSquare"];

const GridSquares = () => {
  console.log("gather grid squares");
  const gridSquares = useLoaderData() as GridSquare[];
  console.log(
    "grid squares",
    gridSquares,
    typeof gridSquares,
    gridSquares.length,
  );
  const { sessid, dcgid } = useParams();

  const getUrl = (endpoint: string) => {
    return (
      (sessionStorage.getItem("murfeyServerURL") ??
        process.env.REACT_APP_API_ENDPOINT) + endpoint
    );
  };

  const res = (
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
                Grid Squares
              </Heading>
            </VStack>
          </VStack>
        </Box>
        <Box
          mt="1em"
          ml="1em"
          w="95%"
          justifyContent={"center"}
          alignItems={"center"}
        >
          {gridSquares && gridSquares.length > 0 ? (
            gridSquares.map((gs) => GridSquareCard(gs, sessid, dcgid))
          ) : (
            <></>
          )}
        </Box>
      </Box>
    </div>
  );
  return res;
};

export { GridSquares };
