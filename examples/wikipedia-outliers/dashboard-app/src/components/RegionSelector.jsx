import { getEmoji } from '../emoji'
import * as classes from './RegionSelector.module.css'

function RegionSelector({ regions, selectedRegion, toggleRegion }) {
  return <ul className={classes.root}>
    {regions.map((region, i) => {
      const selectedClass = selectedRegion && region !== selectedRegion
        ? classes.item__not_selected
        : ''

      return (
        <li
          key={i}
          className={`${classes.item} ${selectedClass}`}
          title={region}
        >
          <button className={classes.button} onClick={() => toggleRegion(region)}>
            {getEmoji(region)}
          </button>
        </li>
      )
    })}
  </ul>
}

export default RegionSelector