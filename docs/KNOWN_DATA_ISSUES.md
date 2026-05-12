# Known data issues

This document tracks known quirks in the source data returned by Allhomes or displayed on Allhomes property pages.

## Property type classification

- Townhouses on split, separate land titles may be returned with `property_type = "HOUSE"` rather than a townhouse-specific property type.

## Allhomes page status

- Some property URLs may show the property status as "Off the market" near the top of the Allhomes page, even though the page later shows that the property was sold. It is not currently clear why the website gives the "Off the market" status precedence over the sold status in these cases. The data returned by the GraphQL endpoint appears to report the sale correctly.
