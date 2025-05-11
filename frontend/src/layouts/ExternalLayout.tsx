import { Outlet } from "react-router-dom";

import Logo from "../components/Logo";

export default function ExternalLayout() {
  return (
    <div className="flex flex-col min-h-screen max-h-screen">
      <div className="flex bg-cyan-600">
        <div className="container mx-auto flex px-3 items-center h-14">
          <Logo />
          <span className="flex-1"></span>
        </div>
      </div>
      <div className="container mx-auto">
        <Outlet />
      </div>
    </div>
  );
}
