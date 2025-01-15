import {
  Button,
  Input,
  VStack,
  Link,
  FormControl,
  Card,
  CardBody,
  Heading,
  HStack,
} from "@chakra-ui/react";
import { Link as LinkRouter, useNavigate, Navigate } from "react-router-dom";
import { TbMicroscope, TbSnowflake } from "react-icons/tb";

import { getJWT, handshake } from "loaders/jwt";

import React from "react";

const Login = () => {
  const [username, setUsername] = React.useState("");
  const handleUsername = (event: React.ChangeEvent<HTMLInputElement>) =>
    setUsername(event.target.value);
  const [password, setPassword] = React.useState("");
  const handlePassword = (event: React.ChangeEvent<HTMLInputElement>) =>
    setPassword(event.target.value);

  const navigate = useNavigate();

  return sessionStorage.getItem("murfeyServerURL") ? (
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
        <HStack>
          {" "}
          <TbSnowflake /> <TbMicroscope />{" "}
        </HStack>{" "}
        Murfey Login
      </Heading>
      <Card>
        <CardBody>
          <FormControl>
            <Input placeholder="Username" onChange={handleUsername} />
            <Input
              placeholder="Password"
              onChange={handlePassword}
              type="password"
            />
            <Button
              onClick={() => {
                getJWT({ username: username, password: password })
                  .then((jwt) =>
                    sessionStorage.setItem("token", jwt.access_token),
                  )
                  .then(() => navigate("/home"));
              }}
            >
              Login
            </Button>
          </FormControl>
        </CardBody>
      </Card>
    </VStack>
  ) : (
    <Navigate to="/hub" replace />
  );
};

export { Login };
