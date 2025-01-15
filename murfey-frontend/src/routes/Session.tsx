import {
  Badge,
  Box,
  Button,
  Card,
  CardBody,
  CardHeader,
  Divider,
  Flex,
  GridItem,
  Heading,
  HStack,
  IconButton,
  Image,
  Link,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalFooter,
  ModalBody,
  ModalCloseButton,
  Spacer,
  Spinner,
  Stack,
  StackDivider,
  Stat,
  StatHelpText,
  StatLabel,
  StatNumber,
  Switch,
  Text,
  Tooltip,
  VStack,
  useToast,
  Icon,
} from "@chakra-ui/react";

import { useDisclosure } from "@chakra-ui/react";
import { ViewIcon } from "@chakra-ui/icons";

import { v4 as uuid4 } from "uuid";
import { Link as LinkRouter, useLoaderData, useParams } from "react-router-dom";
import {
  MdCheck,
  MdDensityMedium,
  MdFileUpload,
  MdOutlineWarning,
  MdOutlineGridOn,
} from "react-icons/md";
import { FiActivity } from "react-icons/fi";
import { components } from "schema/main";
import { getInstrumentName } from "loaders/general";
import {
  pauseRsyncer,
  restartRsyncer,
  removeRsyncer,
  finaliseRsyncer,
} from "loaders/rsyncers";
import { getSessionData } from "loaders/session_clients";
import { InstrumentCard } from "components/instrumentCard";
import { UpstreamVisitCard } from "components/upstreamVisitsCard";
import useWebSocket from "react-use-websocket";

import React, { useEffect } from "react";

type RsyncInstance = components["schemas"]["RsyncInstance"];
type Session = components["schemas"]["Session"];

const RsyncCard = (rsyncer: RsyncInstance) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [action, setAction] = React.useState("finalise");

  const finalise = () => {
    setAction("finalise");
    onOpen();
  };

  const remove = () => {
    setAction("remove");
    onOpen();
  };

  const handleRsyncerAction = async () => {
    if (action === "finalise")
      await finaliseRsyncer(rsyncer.session_id, rsyncer.source);
    else if (action === "remove")
      await removeRsyncer(rsyncer.session_id, rsyncer.source);
    onClose();
  };

  return (
    <Card width="100%" bg="murfey.400" borderColor="murfey.300">
      <Modal isOpen={isOpen} onClose={onClose}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>
            Confirm RSyncer {action}: {rsyncer.source}
          </ModalHeader>
          <ModalCloseButton />
          <ModalBody>Are you sure you want to continue?</ModalBody>

          <ModalFooter>
            <Button colorScheme="blue" mr={3} onClick={onClose}>
              Close
            </Button>
            <Button variant="ghost" onClick={() => handleRsyncerAction()}>
              Confirm
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
      <CardHeader>
        <Flex>
          {" "}
          <Flex flex="1" gap="4" alignItems="center" flexWrap="wrap">
            {" "}
            <HStack spacing="3em">
              <Text>RSync Instance</Text>
              {rsyncer.transferring && <Spinner color="murfey.700" />}
              <Badge
                colorScheme={
                  rsyncer.tag === "fractions"
                    ? "green"
                    : rsyncer.tag === "metadata"
                      ? "purple"
                      : rsyncer.tag === "atlas"
                        ? "yellow"
                        : "red"
                }
              >
                {rsyncer.tag}
              </Badge>
            </HStack>
          </Flex>
          <Menu>
            <MenuButton
              as={IconButton}
              aria-label="Rsync control options"
              icon={<MdDensityMedium />}
            />
            <MenuList>
              {rsyncer.transferring ? (
                <>
                  <MenuItem
                    onClick={() =>
                      pauseRsyncer(rsyncer.session_id, rsyncer.source)
                    }
                    isDisabled={!rsyncer.transferring}
                  >
                    Pause
                  </MenuItem>
                  <MenuItem
                    onClick={() => remove()}
                    isDisabled={!rsyncer.transferring}
                  >
                    Remove
                  </MenuItem>
                  <MenuItem
                    onClick={() => {
                      finalise();
                    }}
                    isDisabled={!rsyncer.transferring}
                  >
                    Finalise
                  </MenuItem>
                </>
              ) : (
                <>
                  <MenuItem
                    onClick={() =>
                      restartRsyncer(rsyncer.session_id, rsyncer.source)
                    }
                  >
                    Start
                  </MenuItem>
                  <MenuItem onClick={() => remove()}>Remove</MenuItem>
                </>
              )}
            </MenuList>
          </Menu>
        </Flex>
      </CardHeader>
      <CardBody>
        <Stack divider={<StackDivider />} spacing="4">
          <Box>
            <Heading size="xs" textTransform="uppercase">
              Source
            </Heading>
            <Text pt="2" fontSize="sm">
              {rsyncer.source}
            </Text>
          </Box>
          <Box>
            <Heading size="xs" textTransform="uppercase">
              Destination
            </Heading>
            <Text pt="2" fontSize="sm">
              {rsyncer.destination ?? ""}
            </Text>
          </Box>
          <Box>
            <Stat>
              <StatLabel>Transfer progress</StatLabel>
              <StatNumber>
                {rsyncer.files_transferred} / {rsyncer.files_counted}
              </StatNumber>
              <StatHelpText>
                {(rsyncer.files_transferred ?? 0) >=
                (rsyncer.files_counted ?? 0) ? (
                  <HStack>
                    <MdCheck />
                    <Text>Up to date</Text>
                  </HStack>
                ) : rsyncer.transferring ? (
                  <HStack>
                    <FiActivity />
                    <Text>Working on it</Text>
                  </HStack>
                ) : (
                  <HStack>
                    <MdOutlineWarning />
                    <Text>Broken</Text>
                  </HStack>
                )}
              </StatHelpText>
            </Stat>
          </Box>
        </Stack>
      </CardBody>
    </Card>
  );
};

const getUrl = (endpoint: string) => {
  return (
    (sessionStorage.getItem("murfeyServerURL") ??
      process.env.REACT_APP_API_ENDPOINT) + endpoint
  );
};

const Session = () => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const rsync = useLoaderData() as RsyncInstance[] | null;
  const { sessid } = useParams();
  const [UUID, setUUID] = React.useState("");
  const [instrumentName, setInstrumentName] = React.useState("");
  const baseUrl =
    sessionStorage.getItem("murfeyServerURL") ??
    process.env.REACT_APP_API_ENDPOINT;
  const url = baseUrl ? baseUrl.replace("http", "ws") : "ws://localhost:8000";
  const toast = useToast();
  const [session, setSession] = React.useState<Session>();

  useEffect(() => {
    getSessionData(sessid).then((sess) => setSession(sess.session));
    setUUID(uuid4());
  }, []);

  const parseWebsocketMessage = (message: any) => {
    let parsedMessage: any = {};
    try {
      parsedMessage = JSON.parse(message);
    } catch (err) {
      return;
    }
    if (parsedMessage.message === "refresh") {
      window.location.reload();
    }
    if (
      parsedMessage.message === "update" &&
      typeof sessid !== "undefined" &&
      parsedMessage.session_id === parseInt(sessid)
    ) {
      return toast({
        title: "Update",
        description: parsedMessage.payload,
        isClosable: true,
        duration: parsedMessage.duration ?? null,
        status: parsedMessage.status ?? "info",
      });
    }
  };

  useWebSocket(url + `ws/connect/${UUID}`, {
    onOpen: () => {
      console.log("WebSocket connection established.");
    },
    onMessage: (event) => {
      parseWebsocketMessage(event.data);
    },
  });

  const finaliseAll = async () => {
    rsync?.map((r) => {
      finaliseRsyncer(r.session_id, r.source);
    });
    onClose();
  };

  const resolveName = async () => {
    const name: string = await getInstrumentName();
    setInstrumentName(name);
  };
  resolveName();

  return (
    <div className="rootContainer">
      <Modal isOpen={isOpen} onClose={onClose}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Confirm Visit Completion</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            Are you sure you want to remove all data associated with this visit?
          </ModalBody>

          <ModalFooter>
            <Button colorScheme="blue" mr={3} onClick={onClose}>
              Close
            </Button>
            <Button variant="ghost" onClick={() => finaliseAll()}>
              Confirm
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
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
                Session {sessid}: {session ? session.visit : null}
              </Heading>
              <HStack>
                <Button variant="onBlue" onClick={() => onOpen()}>
                  Visit Complete
                </Button>
                <Link
                  w={{ base: "100%", md: "19.6%" }}
                  _hover={{ textDecor: "none" }}
                  as={LinkRouter}
                  to={`processing_parameters`}
                >
                  <Button variant="onBlue">Processing Parameters</Button>
                </Link>
                <Spacer />
                <ViewIcon color="white" />
                <Switch colorScheme="murfey" id="monitor" />
                {/* <Button aria-label="Subscribe to notifications" rightIcon={<MdEmail/>} variant="onBlue">
                            Subscribe
                        </Button> */}
              </HStack>
            </VStack>
          </VStack>
        </Box>
        <Box mt="1em" w="95%" justifyContent={"center"} alignItems={"center"}>
          <Flex align="stretch">
            <Stack w="100%" spacing={5} py="0.8em" px="1em">
              {rsync && rsync.length > 0 ? (
                rsync.map((r) => {
                  return RsyncCard(r);
                })
              ) : (
                <GridItem colSpan={5}>
                  <Heading textAlign="center" py={4} variant="notFound">
                    No RSyncers Found
                  </Heading>
                </GridItem>
              )}
            </Stack>
            <Spacer />
            <Stack spacing={5} py="0.8em" px="1em">
              <Link
                w={{ base: "100%", md: "19.6%" }}
                key="data_collections"
                _hover={{ textDecor: "none" }}
                as={LinkRouter}
                to={`../sessions/${sessid}/data_collection_groups`}
              >
                <Button rightIcon={<MdOutlineGridOn />} padding="20px">
                  Data Collections
                </Button>
              </Link>
              <Link
                w={{ base: "100%", md: "19.6%" }}
                key="gain_ref"
                _hover={{ textDecor: "none" }}
                as={LinkRouter}
                to={`../sessions/${sessid}/gain_ref_transfer?sessid=${sessid}`}
              >
                <Button rightIcon={<MdFileUpload />} padding="20px">
                  Transfer Gain Reference
                </Button>
              </Link>
              <InstrumentCard />
              <UpstreamVisitCard sessid={parseInt(sessid ?? "0")} />
            </Stack>
          </Flex>
        </Box>
      </Box>
    </div>
  );
};

export { Session };
