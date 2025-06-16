import { ReactNode } from "react";
import {
  DialogPanel,
  DialogTitle,
  Dialog as HeadlessDialog,
} from "@headlessui/react";
import classNames from "classnames";

const sizeStyles = {
  xs: "max-w-md",
  sm: "max-w-lg",
  md: "max-w-xl",
  lg: "max-w-2xl",
  xl: "max-w-4xl",
  "2xl": "max-w-6xl",
};

type Props = {
  open: boolean;
  title?: ReactNode;
  size?: keyof typeof sizeStyles;
  className?: string;
  onClose: () => void;
  children: ReactNode;
};

export default function Dialog({
  open,
  title,
  size,
  className,
  onClose,
  children,
}: Props) {
  return (
    <HeadlessDialog
      open={open}
      className="fixed inset-0 flex w-screen items-center justify-center bg-black/30 p-4 transition duration-300 ease-out data-closed:opacity-0"
      transition
      onClose={onClose}
    >
      <div
        className={classNames(
          "max-h-screen p-4 flex flex-col w-full items-center",
          size ? sizeStyles[size] : "max-w-screen h-screen",
        )}
      >
        <DialogPanel
          className={classNames(
            "bg-white shadow-xl rounded-lg w-full flex-1 overflow-auto",
            className,
          )}
        >
          {title && (
            <DialogTitle className="text-2xl font-bold text-slate-900 mb-4">
              {title}
            </DialogTitle>
          )}
          {children}
        </DialogPanel>
      </div>
    </HeadlessDialog>
  );
}
