import {
  TabGroup,
  TabList,
  Tab as HeadlessTab,
  TabPanels,
  TabPanel,
} from "@headlessui/react";
import classNames from "classnames";
import { ComponentProps, Fragment, ReactElement, ReactNode } from "react";

type TabProps = {
  label: ReactNode;
  disabled?: boolean;
  className?: string;
  children: ReactNode;
};

export function Tab({ className, children }: TabProps) {
  return <TabPanel className={classNames(className)}>{children}</TabPanel>;
}

type Props = ComponentProps<typeof TabGroup> & {
  className?: string;
  children: ReactElement<TabProps> | ReactElement<TabProps>[];
};

export default function Tabs({ className, children, ...props }: Props) {
  const tabs = Array.isArray(children) ? children : [children];
  return (
    <TabGroup {...props} as={Fragment}>
      <TabList className={classNames("border-b border-slate-200", className)}>
        {tabs.map((c, i) => (
          <HeadlessTab
            key={i}
            disabled={c.props.disabled}
            className="text-sm px-0.5 py-1 border-cyan-500 data-selected:border-b-2 text-slate-600 data-selected:text-slate-900 outline-hidden disabled:opacity-30 not-disabled:cursor-pointer group"
          >
            <span className="block px-1.5 py-0.5 rounded-md group-hover:bg-slate-50">
              {c.props.label}
            </span>
          </HeadlessTab>
        ))}
      </TabList>
      <TabPanels as={Fragment}>{children}</TabPanels>
    </TabGroup>
  );
}
