INSERT overwrite TABLE la_product_details_fact
SELECT prod_detail.fsn AS fsn
	,prod_detail.wid AS wid
	,prod_detail.breadth AS breadth
	,prod_detail.height AS height
	,prod_detail.length AS length
	,prod_detail.shipping_weight AS shipping_weight
	,prod_cat.brand AS brand
	,prod_cat.analytic_vertical AS analytic_vertical
	,prod_cat.title AS title
	,prod_detail.cms_vertical AS cms_vertical
	,prod_detail.seller_flag AS seller_flag
	,prod_detail.created_at AS created_at
	,prod_detail.updated_at AS updated_at
	,prod_detail.price AS price
	,prod_detail.is_dangerous AS is_dangerous
	,prod_detail.is_fragile AS is_fragile
	,prod_detail.seller_id AS seller_id
	,prod_cat.is_large
FROM (
	SELECT `data`.fsn AS fsn
		,`data`.seller_id AS seller_id
		,`data`.wid AS wid
		,`data`.breadth AS breadth
		,`data`.height AS height
		,`data`.length AS length
		,`data`.shipping_weight AS shipping_weight
		,`data`.cms_vertical AS cms_vertical
		,'wsr' AS seller_flag
		,`data`.created_at AS created_at
		,`data`.updated_at AS updated_at
		,`data`.price AS price
		,`data`.is_dangerous AS is_dangerous
		,`data`.is_fragile AS is_fragile
		,`data`.product_title AS product_title
	FROM bigfoot_snapshot.dart_wsr_scp_warehouse_product_detail_3_view_total
	
	UNION ALL
	
	SELECT `data`.fsn AS fsn
		,`data`.seller_id AS seller_id
		,`data`.wid AS wid
		,`data`.breadth AS breadth
		,`data`.height AS height
		,`data`.length AS length
		,`data`.shipping_weight AS shipping_weight
		,`data`.cms_vertical AS cms_vertical
		,'fki' AS seller_flag
		,`data`.created_at AS created_at
		,`data`.updated_at AS updated_at
		,`data`.price AS price
		,`data`.is_dangerous AS is_dangerous
		,`data`.is_fragile AS is_fragile
		,`data`.product_title AS product_title
	FROM bigfoot_snapshot.dart_fki_scp_warehouse_product_detail_2_view_total
	) prod_detail
	LEFT JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim prod_cat
	ON prod_cat.product_id = prod_detail.fsn;
