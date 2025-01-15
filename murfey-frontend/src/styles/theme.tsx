import { ThemeConfig, extendTheme } from "@chakra-ui/react";
import { colours } from "styles/colours";
import {
  Accordion,
  Button,
  Checkbox,
  Heading,
  Table,
  Card,
  Tabs,
  Code,
  Text,
  Input,
  Textarea,
  NumberInput,
  Select,
} from "./components";

const config: ThemeConfig = {
  initialColorMode: "light",
  useSystemColorMode: false,
};

export const theme = extendTheme({
  semanticTokens: { colors: colours },
  components: {
    Accordion,
    Checkbox,
    Button,
    Text,
    Heading,
    Table,
    Card,
    Tabs,
    Code,
    Input,
    Textarea,
    NumberInput,
    Select,
  },
  breakpoints: {
    sm: "30em",
    md: "48em",
    lg: "62em",
    xl: "80em",
    "2xl": "150em",
  },
  config,
});
