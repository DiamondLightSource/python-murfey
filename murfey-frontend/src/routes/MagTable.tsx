import {
  Button,
  Box,
  Heading,
  HStack,
  IconButton,
  Input,
  VStack,
  TableContainer,
  Table,
  Thead,
  Tbody,
  Tfoot,
  Tr,
  Th,
  Td,
  Text,
  TableCaption,
  Modal,
  ModalOverlay,
  ModalHeader,
  ModalFooter,
  ModalContent,
  ModalBody,
  ModalCloseButton,
  FormControl,
  useDisclosure,
} from "@chakra-ui/react";

import { CheckIcon, EditIcon } from "@chakra-ui/icons";

import { Link as LinkRouter, useLoaderData } from "react-router-dom";
import { components } from "schema/main";
import { MdAdd, MdHorizontalRule } from "react-icons/md";
import { addMagTableRow, removeMagTableRow } from "loaders/magTable";

import React from "react";

type MagTableRow = components["schemas"]["MagnificationLookup"];

const MagTable = () => {
  const magTable = useLoaderData() as MagTableRow[] | null;
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [numNewRows, setNumNewRows] = React.useState(0);

  const handleRemoveRow = (mag: number) => {
    removeMagTableRow(mag);
    window.location.reload();
  };

  const handleForm = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    const mag = parseInt(formData.get("magnification") as string);
    const pixelSize = parseFloat(formData.get("pixelSize") as string);
    addMagTableRow(mag, pixelSize);
    window.location.reload();
  };

  return (
    <div className="rootContainer">
      <title>Murfey</title>
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
                Magnification Table
              </Heading>
            </VStack>

            <VStack
              mt="0 !important"
              w="100%"
              px="10vw"
              justifyContent="start"
              alignItems="start"
            >
              <TableContainer>
                <Table variant="simple">
                  <TableCaption>Magnification Table</TableCaption>
                  <Thead>
                    <Tr>
                      <Th>Magnification</Th>
                      <Th>Pixel Size</Th>
                      <Th>Remove</Th>
                    </Tr>
                  </Thead>
                  <Modal isOpen={isOpen} onClose={onClose}>
                    <ModalOverlay />
                    <ModalContent>
                      <ModalHeader>Add Mag Table Row</ModalHeader>
                      <ModalCloseButton />
                      <ModalBody>
                        <form onSubmit={handleForm}>
                          <FormControl>
                            <HStack>
                              <Input
                                placeholder="Magnification"
                                name="magnification"
                              />
                              <Input
                                placeholder="Pixel size (Angstroms)"
                                name="pixelSize"
                              />
                            </HStack>
                          </FormControl>
                          <Button type="submit">Submit</Button>
                        </form>
                      </ModalBody>
                    </ModalContent>
                  </Modal>
                  <Tbody>
                    {magTable && magTable.length > 0 ? (
                      magTable.map((row) => {
                        return (
                          <Tr>
                            <Td>
                              <Text>{row.magnification}</Text>
                            </Td>
                            <Td>
                              <Text>{row.pixel_size}</Text>
                            </Td>
                            <Td>
                              <IconButton
                                aria-label="Remove row from database"
                                icon={<MdHorizontalRule />}
                                onClick={() =>
                                  handleRemoveRow(row.magnification)
                                }
                              ></IconButton>
                            </Td>
                          </Tr>
                        );
                      })
                    ) : (
                      <></>
                    )}
                  </Tbody>
                  <Tfoot>
                    <HStack>
                      <IconButton
                        aria-label="Add row to mag table"
                        icon={<MdAdd />}
                        size="m"
                        onClick={onOpen}
                      ></IconButton>
                    </HStack>
                  </Tfoot>
                </Table>
              </TableContainer>
            </VStack>
          </VStack>
        </Box>
      </Box>
    </div>
  );
};

export { MagTable };
