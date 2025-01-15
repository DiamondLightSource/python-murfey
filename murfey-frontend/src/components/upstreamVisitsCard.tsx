import { Card, CardBody, Button, CardHeader } from "@chakra-ui/react";

import {
  getUpstreamVisits,
  upstreamDataDownloadRequest,
} from "loaders/general";
import { MdFileDownload } from "react-icons/md";

import React, { useEffect } from "react";

interface SessionId {
  sessid: number;
}

const UpstreamVisitCard = ({ sessid }: SessionId) => {
  const [upstreamVisits, setUpstreamVisits] = React.useState({});

  const resolveVisits = async () => {
    const visits = await getUpstreamVisits(sessid);
    setUpstreamVisits(visits);
    console.log(upstreamVisits);
  };
  useEffect(() => {
    resolveVisits();
  }, []);

  return upstreamVisits ? (
    <Card alignItems="center">
      <CardHeader>Upstream Visit Data Download</CardHeader>
      {Object.keys(upstreamVisits).map((k) => {
        return (
          <CardBody>
            <Button
              rightIcon={<MdFileDownload />}
              onClick={() => upstreamDataDownloadRequest(k, sessid)}
            >
              {k}
            </Button>
          </CardBody>
        );
      })}
    </Card>
  ) : (
    <></>
  );
};

export { UpstreamVisitCard };
