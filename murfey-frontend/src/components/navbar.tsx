import {
  Box,
  Flex,
  HStack,
  Link,
  IconButton,
  useDisclosure,
  Image,
  Tooltip,
  VStack,
  BoxProps,
  Icon,
} from "@chakra-ui/react";
import {
  MdMenu,
  MdClose,
  MdSignalWifi4Bar,
  MdOutlineSignalWifiBad,
} from "react-icons/md";
import { TbMicroscope, TbSnowflake } from "react-icons/tb";
import { getInstrumentConnectionStatus } from "loaders/general";
import { Link as LinkRouter } from "react-router-dom";
import React from "react";

export interface LinkDescriptor {
  label: string;
  route: string;
}

interface BaseLinkProps {
  links?: LinkDescriptor[];
  as?: React.ElementType;
}

export interface NavbarProps extends BaseLinkProps, BoxProps {
  logo?: string | null;
  children?: React.ReactElement;
}

const NavLinks = ({ links, as }: BaseLinkProps) => (
  <>
    {links
      ? links.map((link) => (
          <Link
            height="100%"
            alignItems="center"
            display="flex"
            px={2}
            textDecor="none"
            as={as}
            borderTop="4px solid transparent"
            borderBottom="4px solid transparent"
            color="murfey.50"
            _hover={{
              color: "murfey.500",
              borderBottom: "solid 4px",
            }}
            to={link.route}
            key={link.label}
          >
            {link.label}
          </Link>
        ))
      : null}
  </>
);

const Navbar = ({ links, as, children, logo, ...props }: NavbarProps) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [instrumentConnectionStatus, setInsrumentConnectionStatus] =
    React.useState(false);

  const resolveConnectionStatus = async () => {
    const status: boolean = await getInstrumentConnectionStatus();
    setInsrumentConnectionStatus(status);
  };
  resolveConnectionStatus();

  return (
    <Box position="sticky" top="0" zIndex={1} w="100%" {...props}>
      <Flex
        bg="murfey.800"
        px={{ base: 4, md: "7.5vw" }}
        h={12}
        alignItems={"center"}
        justifyContent={"space-between"}
      >
        <IconButton
          size={"sm"}
          icon={isOpen ? <MdClose /> : <MdMenu />}
          aria-label={"Open Menu"}
          display={{ md: "none" }}
          bg="transparent"
          border="none"
          _hover={{ background: "transparent", color: "murfey.500" }}
          onClick={isOpen ? onClose : onOpen}
        />
        <HStack h="100%" spacing={8} alignItems={"center"}>
          {logo ? (
            <Link as={LinkRouter} to="/home">
              <Box maxW="5rem">
                <Image
                  alt="Home"
                  _hover={{ filter: "brightness(70%)" }}
                  fit="cover"
                  paddingBottom={{ md: "6px", base: 0 }}
                  src={logo}
                />
              </Box>
            </Link>
          ) : null}
          <Link as={LinkRouter} to="/hub">
            <Tooltip label="Back to the Hub">
              <IconButton
                size={"sm"}
                icon={
                  <>
                    <TbSnowflake />
                    <TbMicroscope />
                  </>
                }
                aria-label={"Back to the Hub"}
                _hover={{ background: "transparent", color: "murfey.500" }}
              />
            </Tooltip>
          </Link>
          <HStack
            h="100%"
            as={"nav"}
            spacing={4}
            display={{ base: "none", md: "flex" }}
          >
            <NavLinks links={links} as={as} />
          </HStack>
          <Tooltip
            label={
              instrumentConnectionStatus
                ? "Connected to instrument server"
                : "No instrument server connection"
            }
            placement="bottom"
          >
            <Icon
              as={
                instrumentConnectionStatus
                  ? MdSignalWifi4Bar
                  : MdOutlineSignalWifiBad
              }
              color={instrumentConnectionStatus ? "white" : "red"}
            />
          </Tooltip>
        </HStack>
        <Flex alignItems={"center"}>{children}</Flex>
      </Flex>
      {isOpen && (
        <VStack
          bg="murfey.700"
          borderBottom="1px solid"
          borderColor="murfey.500"
          as={"nav"}
          spacing={4}
        >
          <NavLinks links={links} as={as} />
        </VStack>
      )}
    </Box>
  );
};

export { Navbar };
