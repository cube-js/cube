import React, { useState, useEffect } from "react";
import { DatePicker } from "@material-ui/pickers";
import { useUtils } from "@material-ui/pickers";
import { fade } from "@material-ui/core/styles";
import withStyles from "@material-ui/core/styles/withStyles";
import clsx from "clsx";

function DateRangePicker({
  classes,
  value,
  onChange,
  labelFunc,
  format,
  emptyLabel,
  autoOk,
  onOpen,
  onClose,
  open: openForward,
  ...props
}) {
  const [begin, setBegin] = useState(value[0]);
  const [end, setEnd] = useState(value[1]);
  const [prevBegin, setPrevBegin] = useState(undefined);
  const [prevEnd, setPrevEnd] = useState(undefined);
  const [hasClicked, setHasClicked] = useState(false);

  const [hover, setHover] = useState(undefined);
  const [accepted, setAccepted] = useState(!!begin && !!end);

  const utils = useUtils();

  const min = Math.min(begin, end || hover);
  const max = Math.max(begin, end || hover);

  const [open, setOpen] = useState(false);

  const isOpen = openForward !== undefined ? openForward : open;

  useEffect(() => {
    //Only way to get to this state is is openForward is used
    if (isOpen && accepted && !prevBegin && !prevEnd) {
      setAccepted(false);
      setPrevBegin(begin);
      setPrevEnd(end);
      return;
    }
    //Closed without accepting, reset to prev state, don't find onChange
    if (!isOpen && !accepted) {
      setBegin(prevBegin);
      setEnd(prevEnd);
      setHover(undefined);
      setHasClicked(false);
    }
    //Auto ok and hasn't been accepted, but has all the items set, accept and close.
    //This will also triger the on change event by setting isOpen to false
    if (isOpen && autoOk && !accepted && begin && end && hasClicked) {
      setAccepted(true);
      onClose ? onClose() : setOpen(false);
    }
    if (accepted && begin && end && !isOpen && hasClicked) {
      setHasClicked(false);
      onChange({ begin, end });
      onClose ? onClose() : setOpen(false);
    }
  }, [begin, end, autoOk, accepted, isOpen, prevBegin, hasClicked, prevEnd]);

  function renderDay(day, selectedDate, dayInCurrentMonth, dayComponent) {
    return React.cloneElement(dayComponent, {
      onClick: e => {
        setHasClicked(true);
        e.stopPropagation();
        if (!begin) setBegin(day);
        else if (!end) {
          if (utils.isBeforeDay(day, begin)) {
            setEnd(begin);
            setBegin(day);
          } else {
            setEnd(day);
          }
          if (autoOk) {
            setPrevBegin(undefined);
            setPrevEnd(undefined);
          }
        } else {
          setBegin(day);
          setEnd(undefined);
        }
      },
      onMouseEnter: () => requestAnimationFrame(() => setHover(day)),
      onFocus: () => requestAnimationFrame(() => setHover(day)),
      className: clsx(classes.day, {
        [classes.hidden]: dayComponent.props.hidden,
        [classes.current]: dayComponent.props.current,
        [classes.isDisabled]: dayComponent.props.disabled,
        [classes.focusedRange]:
          (utils.isAfterDay(day, min) && utils.isBeforeDay(day, max)) ||
          (utils.isSameDay(day, min) && !utils.isSameDay(day, max)) ||
          (utils.isSameDay(day, max) && !utils.isSameDay(day, min)),
        [classes.focusedFirst]:
          utils.isSameDay(day, min) && !utils.isSameDay(day, max),
        [classes.focusedLast]:
          utils.isSameDay(day, max) && !utils.isSameDay(day, min),
        [classes.beginCap]: utils.isSameDay(day, min),
        [classes.endCap]: utils.isSameDay(day, max)
      })
    });
  }

  const formatDate = date => utils.format(date, format || utils.dateFormat);

  return (
    <DatePicker
      fullWidth={true}
      {...props}
      value={begin}
      renderDay={renderDay}
      open={isOpen}
      onOpen={() => {
        setAccepted(false);
        setPrevBegin(begin);
        setPrevEnd(end);
        onOpen ? onOpen() : setOpen(true);
      }}
      onAccept={() => {
        if (!begin || !end) {
          if (hover && utils.isBeforeDay(begin, hover)) {
            setEnd(hover);
          } else {
            setEnd(begin);
            setBegin(hover);
          }
        }
        setPrevBegin(undefined);
        setPrevEnd(undefined);
        // if (!autoOk) {
        setAccepted(true);
        // }
      }}
      onClose={() => {
        onClose ? onClose() : setOpen(false);
      }}
      onChange={() => {}}
      labelFunc={(date, invalid) =>
        !isOpen
          ? labelFunc
            ? labelFunc([begin, end], invalid)
            : date && begin && end
            ? `${formatDate(begin)} - ${formatDate(end)}`
            : emptyLabel || ""
          : prevBegin && prevEnd
          ? labelFunc
            ? labelFunc([prevBegin, prevEnd], invalid)
            : `${formatDate(prevBegin)} - ${formatDate(prevEnd)}`
          : emptyLabel || ""
      }
      DialogProps={{ className: classes.dateRangePickerDialog }}
    />
  );
}

export const styles = theme => {
  const focusedRangeColor = fade(theme.palette.primary.main, 0.3);
  const focusedRangeGradient = `linear-gradient(to right, ${focusedRangeColor}, ${focusedRangeColor})`;
  const transparentRangeGradient = `linear-gradient(to right, rgba(0,0,0,0.0), rgba(0,0,0,0.0))`;
  return {
    dateRangePickerDialog: {
      "& .MuiPickersCalendar-transitionContainer": {
        minHeight: 218,
        marginTop: 10
      }
    },
    day: {
      width: 40,
      height: 36,
      fontSize: theme.typography.caption.fontSize,
      margin: 0,
      color: theme.palette.text.primary,
      fontWeight: theme.typography.fontWeightMedium,
      padding: 0,
      transition: "none",
      "&::after": {
        borderRadius: "100%",
        bottom: 0,
        boxSizing: "border-box",
        content: '""',
        height: 36,
        width: 36,
        left: 0,
        margin: "auto",
        position: "absolute",
        right: 0,
        top: 0,
        transform: "scale(0)",
        zIndex: 2
      },
      "&:hover": {
        backgroundColor: "transparent",
        color: theme.palette.text.primary,
        "&::after": {
          backgroundColor: theme.palette.background.paper,
          border: `2px solid ${theme.palette.primary.main}`,
          bottom: -2,
          left: -2,
          height: 36,
          width: 36,
          right: -2,
          top: -2,
          boxSizing: "content-box",
          transform: "scale(1)"
        }
      },
      "& > .MuiIconButton-label": {
        zIndex: 3
      }
    },
    hidden: {
      opacity: 0,
      pointerEvents: "none"
    },
    current: {
      color: theme.palette.primary.main,
      fontWeight: 600
    },
    focusedRange: {
      color: theme.palette.primary.contrastText,
      background: `${focusedRangeGradient} no-repeat 0/20px 40px, ${focusedRangeGradient} no-repeat 20px 0/20px 40px`,
      fontWeight: theme.typography.fontWeightMedium,
      width: 40,
      marginRight: 0,
      marginLeft: 0,
      borderRadius: 0
    },
    dayDisabled: {
      pointerEvents: "none",
      color: theme.palette.text.hint
    },
    beginCap: {
      "&::after": {
        transform: "scale(1)",
        backgroundColor: theme.palette.primary.main
      }
    },
    endCap: {
      "&::after": {
        transform: "scale(1)",
        backgroundColor: theme.palette.primary.main
      }
    },
    focusedFirst: {
      background: `${transparentRangeGradient} no-repeat 0/20px 40px,${focusedRangeGradient} no-repeat 20px 0/20px 40px`
    },
    focusedLast: {
      background: `${focusedRangeGradient} no-repeat 0/20px 40px,${transparentRangeGradient} no-repeat 20px 0/20px 40px`
    }
  };
};

DateRangePicker.defaultProps = {
  format: 'MMM D, YYYY'
}

export default withStyles(styles, { name: "DateRangePicker" })(DateRangePicker);
