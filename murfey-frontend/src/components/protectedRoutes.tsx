import { Navigate, Outlet } from "react-router-dom";
import { Box } from "@chakra-ui/react";
import { Navbar } from "components/navbar";

const ProtectedRoutes = () => {
  const sessionToken = sessionStorage.getItem("token");
  const standard = (
    <div className="rootContainer">
      <Box>
        <Navbar logo="/images/diamondgs.png" />
      </Box>
      <Box className="main">
        <Outlet />
      </Box>
    </div>
  );
  return sessionToken ? standard : <Navigate to="/login" replace />;
};

export { ProtectedRoutes };
