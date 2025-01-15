const timeFormatter = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "short",
  timeStyle: "short",
});

export const parseDate = (dateString: string | undefined) => {
  const safeDate = dateString ?? "";
  const date = Date.parse(safeDate);

  if (isNaN(date)) {
    return safeDate;
  }

  return timeFormatter.format(date);
};
