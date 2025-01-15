import { Box, RadioGroup, Radio, Stack } from "@chakra-ui/react";
import { getForm } from "components/forms";

import React from "react";

const UserParameters = () => {
  const [expType, setExpType] = React.useState("spa");
  const [procParams, setProcParams] = React.useState();
  return (
    <Stack>
      <Box
        borderWidth="1px"
        borderRadius="lg"
        overflow="hidden"
        padding="10px"
        display="flex"
        borderColor={"murfey.400"}
      >
        <RadioGroup onChange={setExpType} value={expType} colorScheme="murfey">
          <Stack>
            <Radio value="spa">SPA</Radio>
            <Radio value="tomography">Tomography</Radio>
          </Stack>
        </RadioGroup>
      </Box>
      <Box
        borderWidth="1px"
        borderRadius="lg"
        overflow="hidden"
        padding="10px"
        display="flex"
        borderColor={"murfey.400"}
      >
        {getForm(expType, setProcParams)}
      </Box>
    </Stack>
  );
};

export { UserParameters };
