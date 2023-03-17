import { DB_CAST } from './CAST';

export const sql = (type) => {
  const { SELECT_PREFIX, SELECT_SUFFIX } = DB_CAST[type];
  const select = `
    select 'Furniture' as category, 'Tables' as sub_category, 'Anderson Hickey Conga Table Tops & Accessories' as product_name union all
    select 'Furniture' as category, 'Tables' as sub_category, 'Balt Solid Wood Rectangular Table' as product_name union all
    select 'Furniture' as category, 'Bookcases' as sub_category, 'DMI Eclipse Executive Suite Bookcases' as product_name union all
    select 'Furniture' as category, 'Bookcases' as sub_category, 'Global Adaptabilites Bookcase, Cherry/Storm Gray Finish' as product_name union all
    select 'Furniture' as category, 'Chairs' as sub_category, 'Harbour Creations 67200 Series Stacking Chairs' as product_name union all
    select 'Furniture' as category, 'Chairs' as sub_category, 'Iceberg Nesting Folding Chair, 19w x 6d x 43h' as product_name union all
    select 'Furniture' as category, 'Furnishings' as sub_category, 'Linden 10 Round Wall Clock, Black' as product_name union all
    select 'Furniture' as category, 'Furnishings' as sub_category, 'Magna Visual Magnetic Picture Hangers' as product_name union all
    select 'Office Supplies' as category, 'Art' as sub_category, 'OIC #2 Pencils, Medium Soft' as product_name union all
    select 'Office Supplies' as category, 'Art' as sub_category, 'Panasonic KP-380BK Classic Electric Pencil Sharpener' as product_name union all
    select 'Office Supplies' as category, 'Storage' as sub_category, 'Project Tote Personal File' as product_name union all
    select 'Office Supplies' as category, 'Storage' as sub_category, 'Recycled Eldon Regeneration Jumbo File' as product_name union all
    select 'Office Supplies' as category, 'Envelopes' as sub_category, 'Tyvek Side-Opening Peel & Seel Expanding Envelopes' as product_name union all
    select 'Office Supplies' as category, 'Envelopes' as sub_category, 'Wausau Papers Astrobrights Colored Envelopes' as product_name union all
    select 'Office Supplies' as category, 'Fasteners' as sub_category, 'Vinyl Coated Wire Paper Clips in Organizer Box, 800/Box' as product_name union all
    select 'Office Supplies' as category, 'Fasteners' as sub_category, 'Plymouth Boxed Rubber Bands by Plymouth' as product_name union all
    select 'Technology' as category, 'Accessories' as sub_category, 'Logitech diNovo Edge Keyboard' as product_name union all
    select 'Technology' as category, 'Accessories' as sub_category, 'Kingston Digital DataTraveler 16GB USB 2.0' as product_name union all
    select 'Technology' as category, 'Accessories' as sub_category, 'Kingston Digital DataTraveler 16GB USB 2.1' as product_name union all
    select 'Technology' as category, 'Accessories' as sub_category, 'Kingston Digital DataTraveler 16GB USB 2.2' as product_name union all
    select 'Technology' as category, 'Phones' as sub_category, 'Google Nexus 5' as product_name union all
    select 'Technology' as category, 'Phones' as sub_category, 'Google Nexus 6' as product_name union all
    select 'Technology' as category, 'Phones' as sub_category, 'Google Nexus 7' as product_name union all
    select 'Technology' as category, 'Phones' as sub_category, 'HTC One' as product_name union all
    select 'Technology' as category, 'Copiers' as sub_category, 'Canon PC1080F Personal Copier' as product_name union all
    select 'Technology' as category, 'Copiers' as sub_category, 'Hewlett Packard 610 Color Digital Copier / Printer' as product_name union all
    select 'Technology' as category, 'Machines' as sub_category, 'Lexmark 20R1285 X6650 Wireless All-in-One Printer' as product_name union all
    select 'Technology' as category, 'Machines' as sub_category, 'Okidata C610n Printer' as product_name
  `;
  return SELECT_PREFIX + select + SELECT_SUFFIX;
};
