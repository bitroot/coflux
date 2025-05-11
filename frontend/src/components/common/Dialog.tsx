import { ReactNode } from "react";
import {
  DialogPanel,
  DialogTitle,
  Dialog as HeadlessDialog,
} from "@headlessui/react";
import classNames from "classnames";

type Props = {
  open: boolean;
  title?: ReactNode;
  className?: string;
  onClose: () => void;
  children: ReactNode;
};

export default function Dialog({
  title,
  open,
  className = "p-6",
  onClose,
  children,
}: Props) {
  return (
    <HeadlessDialog
      open={open}
      className="fixed inset-0 flex w-screen items-center justify-center bg-black/30 p-4 transition duration-300 ease-out data-[closed]:opacity-0"
      transition
      onClose={onClose}
    >
      <DialogPanel
        className={classNames(
          "bg-white shadow-xl rounded-lg w-full",
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
    </HeadlessDialog>
  );
}
