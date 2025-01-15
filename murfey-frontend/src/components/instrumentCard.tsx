import {
  Card,
  CardBody,
  CardHeader,
  Image,
  Link,
  Text,
} from "@chakra-ui/react";

import { Link as LinkRouter } from "react-router-dom";
import { getInstrumentName } from "loaders/general";

import React, { useEffect } from "react";

const getUrl = (endpoint: string) => {
  return (
    (sessionStorage.getItem("murfeyServerURL") ??
      process.env.REACT_APP_API_ENDPOINT) + endpoint
  );
};

const InstrumentCard = () => {
  const [instrumentName, setInstrumentName] = React.useState("");

  const resolveName = async () => {
    const name: string = await getInstrumentName();
    setInstrumentName(name);
  };
  useEffect(() => {
    resolveName();
  }, []);

  return (
    <Link
      key="ag_table"
      _hover={{ textDecor: "none" }}
      as={LinkRouter}
      to={`../mag_table`}
    >
      <Card align="center">
        <CardHeader>
          <Image
            src={getUrl(
              `display/instruments/${sessionStorage.getItem(
                "instrumentName",
              )}/image/`,
            )}
          />
        </CardHeader>
        <CardBody>
          <Text>{instrumentName}</Text>
        </CardBody>
      </Card>
    </Link>
  );
};

export { InstrumentCard };
