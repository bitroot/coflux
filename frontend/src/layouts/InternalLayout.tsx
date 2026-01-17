import { Outlet } from "react-router-dom";
import { SocketProvider } from "@topical/react";

const API_VERSION = "0.8";

export default function InternalLayout() {
  return (
    <SocketProvider
      url={`ws://${window.location.host}/topics?version=${API_VERSION}`}
    >
      <div className="flex flex-col min-h-screen max-h-screen overflow-hidden bg-white">
        <Outlet />
      </div>
    </SocketProvider>
  );
}
