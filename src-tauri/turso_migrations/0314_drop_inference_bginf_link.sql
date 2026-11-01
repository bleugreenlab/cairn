-- lineage: private
-- cairn/5720: bginf retired. Its link row held the last raw responses of the
-- poller that mirrored bginf's control service into the inference records;
-- the poller is gone, the records are Cairn's own once adopted, and nothing
-- reads the row.
DROP TABLE inference_bginf_link;
