import { WarningBox } from './AlertBox'

export const CommunitySupportedDriver = ({ dataSource }) => {
  return (
    <WarningBox>
      The driver for {dataSource} is{' '}
      <a href="/product/configuration/data-sources#driver-support">
        community-supported
      </a>{' '}
      and is not supported by Cube or the vendor.
    </WarningBox>
  )
}
