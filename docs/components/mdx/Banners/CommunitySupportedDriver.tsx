import { WarningBox } from "@/components/mdx/AlertBox/AlertBox";
import { Link } from "@/components/overrides/Anchor/Link";

export interface CommunitySupportedDriverProps {
  dataSource: string;
}

export const CommunitySupportedDriver = ({
  dataSource,
}: CommunitySupportedDriverProps) => {
  return (
    <WarningBox>
      The driver for {dataSource} is{" "}
      <Link href="/product/configuration/data-sources#driver-support">
        community-supported
      </Link>{" "}
      and is not supported by Cube or the vendor.
    </WarningBox>
  );
};
