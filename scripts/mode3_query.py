Mod3_query = """-- #########################################################################
-- OL Optimisation SQL Code
-- Developed by Adolfo Acosta for FW Engineering
-- FW Performance - December 2023
----------------------------------------------------------------------------
-- 12 Beam Mini Lens included January 2024
-- Added filters to remove junk registries from all M3 available views
-- Workaround to solve date formating issues causing invalid output February 2024
-- Solved PCI issues causing WNTDs being excluded from output
-- Updated Antenna table with Mini Lens-HP-MQ4 antenna type March 2023
-- Solved "Cannot cast '#NAME?' to DOUBLE" error causing query to fail
-- Added exceptions for incorrect date and rubbish registry handling         
-- #########################################################################
--
with mod_dataFrame as
--
(
--
with main_input as
--
(
-- #### Get device information
with qry_users as
--
(select dt as "dt_users"
, cluster_name
, imsi
, location_id
, wntd_id
, wntd_version
, gps_location_latitude
, gps_location_longitude
, cell_enb_logical_name
, eutran_cell_id
, site
from hive.thor_fw.v_cur_fw_wntd_enrichment)
--
,
-- #### Get the total number of in service devices per cell
qry_customers as
--
(select eutran_cell_id
, count(wntd_id) as "connected_devices" 
from
hive.thor_fw.v_cur_eps_inventory_installed_wntd
where  total_active_avc > 0
group by
eutran_cell_id)
--
,
-- #### M3 data information
qry_m3 as
--
(select qry_m3_maxDate.newest_m3_date
, qry_m3_maxDate.imsi
, qry_m3_all.pci
, qry_m3_all.rsrp
, qry_m3_all.cinr
, qry_m3_all.rsrq
, qry_m3_all.earfcn
, qry_m3_all.channel_flag
, qry_m3_all.reference_value
--
from
--
(select imsi, max(new_date) as "newest_m3_date"
from 
(select date_collected
, (case when length(date_collected) < 10 then cast(replace(substring(date_collected,6,4)||substring(date_collected,3,2)||'0'||substring(date_collected,1,1),'/','') as integer)
        when length(date_collected) > 9 then cast(replace(substring(date_collected,7,4)||substring(date_collected,4,2)||substring(date_collected,1,2),'/','') as integer) end) as "new_date"
, imsi
, pci
, rsrp
, cinr
, rsrq
, earfcn 
from
--hive_user_upload.thor_user_files.albanspella_barberet_mode3_current
hive_user_upload.thor_user_files.huepham1_mode3_current
where date_collected not in ('combinecsv.csv'
, 'combinexlsx.xlsx'
, 'Mode3.xlsx'
, 'Mode3_CRQ000006198323_Lawrence.xlsx'
, 'Mode3_CRQ000006272909.xlsx'
, 'Mode3_CRQ000006110712.xlsx') 
and imsi not in ('dummy', 'imsi')
and pci not in ('|', 'PCI', 'dummy', 'V3-NA')
and rsrp not like ('#NAME?'))
--where imsi like '505626260576462'
group by imsi) qry_m3_maxDate
--
,
--
(select m3_b.date_collected
, m3_b.new_date
, m3_b.imsi
, m3_b.pci
, m3_b.rsrp
, m3_b.cinr
, m3_b.rsrq
, m3_b.earfcn
, m3_a.channel_flag
, m3_b.reference_value
, m3_b.date_inverted
--
from
--
(select new_date, imsi_earfcn, count(imsi_earfcn) as "channel_flag" -- detect earfcn clashes    
--
from
--
(select date_collected
, (case when length(date_collected) < 10 then cast(replace(substring(date_collected,6,4)||substring(date_collected,3,2)||'0'||substring(date_collected,1,1),'/','') as integer)
        when length(date_collected) > 9 then cast(replace(substring(date_collected,7,4)||substring(date_collected,4,2)||substring(date_collected,1,2),'/','') as integer) end) as "new_date"
, imsi
, pci
, rsrp
, cinr
, rsrq
, earfcn
, imsi||'_'||cast(earfcn as varchar) as "imsi_earfcn"
, cast(split_part(pci,'.',1) as varchar)||'_'||cast(earfcn as varchar) as "reference_value"
, (case when length(date_collected) < 10 then cast(CONCAT(SUBSTRING(replace(date_collected,'/',''), 4, 4)
, SUBSTRING(replace(date_collected,'/',''), 2, 2)
, '0',SUBSTRING(replace(date_collected,'/',''), 1, 1)) as integer) 
        when length(date_collected) > 9 then cast(CONCAT(SUBSTRING(replace(date_collected,'/',''), 5, 4)
, SUBSTRING(replace(date_collected,'/',''), 3, 2)
, SUBSTRING(replace(date_collected,'/',''), 1, 2)) as integer) end) as "date_inverted"
from
-- hive_user_upload.thor_user_files.albanspella_barberet_mode3_current
hive_user_upload.thor_user_files.huepham1_mode3_current
where date_collected not in ('combinecsv.csv'
, 'combinexlsx.xlsx'
, 'Mode3.xlsx'
, 'Mode3_CRQ000006198323_Lawrence.xlsx'
, 'Mode3_CRQ000006272909.xlsx'
, 'Mode3_CRQ000006110712.xlsx') 
and imsi not in ('dummy', 'imsi')
and pci not in ('|', 'PCI', 'dummy', 'V3-NA')
and rsrp not like ('#NAME?')
group by date_collected
, imsi
, pci
, rsrp
, cinr
, rsrq
, earfcn)
--
group by new_date, imsi_earfcn) m3_a
--
,
--
(select date_collected
, (case when length(date_collected) < 10 then cast(replace(substring(date_collected,6,4)||substring(date_collected,3,2)||'0'||substring(date_collected,1,1),'/','') as integer)
        when length(date_collected) > 9 then cast(replace(substring(date_collected,7,4)||substring(date_collected,4,2)||substring(date_collected,1,2),'/','') as integer) end) as "new_date"
, imsi
, pci
, rsrp
, cinr
, rsrq
, earfcn
, imsi||'_'||cast(earfcn as varchar) as "imsi_earfcn"
, cast(split_part(pci,'.',1) as varchar)||'_'||cast(earfcn as varchar) as "reference_value"
, (case when length(date_collected) < 10 then cast(CONCAT(SUBSTRING(replace(date_collected,'/',''), 4, 4)
, SUBSTRING(replace(date_collected,'/',''), 2, 2)
, '0',SUBSTRING(replace(date_collected,'/',''), 1, 1)) as integer) 
        when length(date_collected) > 9 then cast(CONCAT(SUBSTRING(replace(date_collected,'/',''), 5, 4)
, SUBSTRING(replace(date_collected,'/',''), 3, 2)
, SUBSTRING(replace(date_collected,'/',''), 1, 2)) as integer) end) as "date_inverted"
from
-- hive_user_upload.thor_user_files.albanspella_barberet_mode3_current
hive_user_upload.thor_user_files.huepham1_mode3_current
where date_collected not in ('combinecsv.csv'
, 'combinexlsx.xlsx'
, 'Mode3.xlsx'
, 'Mode3_CRQ000006198323_Lawrence.xlsx'
, 'Mode3_CRQ000006272909.xlsx'
, 'Mode3_CRQ000006110712.xlsx') 
and imsi not in ('dummy', 'imsi')
and pci not in ('|', 'PCI', 'dummy', 'V3-NA')
and rsrp not like ('#NAME?')
group by date_collected
, imsi
, pci
, rsrp
, cinr
, rsrq
, earfcn) m3_b
--
where m3_a.new_date = m3_b.new_date and m3_a.imsi_earfcn = m3_b.imsi_earfcn 
--
group by
--
m3_b.date_collected
, m3_b.new_date
, m3_b.imsi
, m3_b.pci
, m3_b.rsrp
, m3_b.cinr
, m3_b.rsrq
, m3_b.earfcn
, m3_a.channel_flag
, m3_b.reference_value
, m3_b.date_inverted) qry_m3_all
--
where qry_m3_all.imsi not in ('Dummy','505626260235660')
and qry_m3_all.imsi = qry_m3_maxDate.imsi 
and qry_m3_all.new_date = qry_m3_maxDate.newest_m3_date
and qry_m3_all.date_inverted >= cast(to_char(now() - interval '30' day,'yyyymmdd') as integer))  -- #### Exclude scan data older than 30 days
--
,
-- #### Relevant cell information 
qry_cells as
-- 
(select dt
, e_utran_cell
, pci
, earfcn
, enodeb
, administrative_state
, cell_barred
, operational_state
, 'B'||cast(freq_band as varchar) as "cell_band"
, cast(pci as varchar)||'_'||cast(earfcn as varchar) as "reference_value"
from hive.thor_fw.v_cur_enm_ran_topology_e_utran_cell_tdd
where administrative_state like 'UNLOCKED' 
and cell_barred like 'NOT_BARRED' 
and operational_state like 'ENABLED')
--
,
-- #### Get serving site infomation
qry_sites as
--
(select eutran_cell_id
, cell_pci
, antenna_type
, site_name
, program_of_work
, cell_pci||'_'||substr(cell_frequency_mhz,-5) as "reference_value"
, cast(actual_latitude as double) as "t_lat"
, cast(actual_longitude as double) as "t_lon"
from
hive.thor_fw.v_cur_appian_cell_enrichment_all
where pow_status = 'Latest'
	  and trim(site_name) not like 'PVT%' 		
      and program_of_work not like 'PVT%'
	  and site_name not like 'D-%'		
	  and coalesce(hold_status,'null') not like '%Terminated%'
	  and eutran_cell_id is not null
	  and cell_status is null)
--
,
-- #### Get AVC traffic
qry_traffic as
--
(select poi_site_code
, avc_id
, location_id
, sum(upstream_bytes)/1000000000 as "AVC_UL_GB"
, sum(downstream_bytes)/1000000000 as "AVC_DL_GB"
from hive.thor_agg.v_agg_thor_avc_octets_traffic_class_summary
where dt >= cast(date_format(current_date - interval '14' day, '%Y%m%d') as integer)
and poi_site_code is not NULL
and access_service_tech_type = 'Fixed Wireless'
group by
poi_site_code
, avc_id
, location_id
having sum(downstream_bytes) > 0)
--
-- #### Field selection for output file
--
select *
, dense_rank() OVER (ORDER BY U2S_Distance_Km) as "distance_rank"
--
from
--
(select qry_u.dt_users
, qry_m3.newest_m3_date
, qry_u.wntd_version
, qry_u.wntd_id
, qry_m3.imsi
, qry_u.location_id
, qry_t.avc_id
, qry_u.eutran_cell_id as "s_cell"
, qry_u.site as "s_site"
, qry_m3.pci
, qry_m3.rsrp
, qry_m3.cinr
, qry_m3.rsrq
, qry_m3.earfcn
, qry_m3.reference_value
, qry_m3.channel_flag
, qry_t.poi_site_code
, qry_u.cluster_name
, qry_t.AVC_UL_GB
, qry_t.AVC_DL_GB
, qry_u.gps_location_latitude
, qry_u.gps_location_longitude
, qry_ce.e_utran_cell as "t_cell"
, qry_ce.cell_band as "t_cell_band"
, qry_cu.connected_devices
, qry_s.antenna_type
, qry_s.antenna_type||'_'||qry_ce.cell_band as "antenna_cell_band"
, qry_s.site_name as "t_site"
, qry_s.t_lat
, qry_s.t_lon
, Round(ST_Distance(to_spherical_geography(ST_Point(qry_u.gps_location_longitude, qry_u.gps_location_latitude))
, to_spherical_geography(ST_Point(cast(qry_s.t_lon as double), cast(qry_s.t_lat as double))))/1000,5) as "U2S_Distance_Km"
, case when qry_u.eutran_cell_id = qry_ce.e_utran_cell then 1 else 0 end as "t_cell_check" 
--
from qry_m3
--
left join qry_users qry_u on qry_u.imsi = qry_m3.imsi
left join qry_traffic qry_t on qry_u.location_id = qry_t.location_id
left join qry_cells qry_ce on qry_ce.reference_value = qry_m3.reference_value
left join qry_sites qry_s on qry_s.eutran_cell_id = qry_ce.e_utran_cell
left join qry_customers qry_cu on qry_ce.e_utran_cell = qry_cu.eutran_cell_id)
--
where U2S_Distance_Km <= 30) -- #### Set distance value to filter outliars
--
,
-- #### Site to user bearing calculations. This is a code block of 4 queries...
pi_Calc as 
--
(select main_input.t_site
, main_input.t_cell
, main_input.wntd_id
, (pi() / 180) * main_input.t_lat as "c_lat"
, (pi() / 180) * main_input.t_lon as "c_lon"
, (pi() / 180) * main_input.gps_location_latitude as "w_lat"
, (pi() / 180) * main_input.gps_location_longitude as "w_lon"
from main_input)
--
,
--
rad_Calc as
--
(select t_site
, t_cell
, wntd_id
, radians(w_lat - c_lat) as "d_lat"
, radians(w_lon - c_lon) as "d_lon"
, radians(c_lat) as "r_cLat"
, radians(w_lat) as "r_wLat"
from pi_Calc)
--
,
--
xy_Calc as
--
(select t_site
, t_cell
, wntd_id
, sin(d_lon) * cos(r_wLat) as "Y"
, cos(r_cLat) * sin(r_wLat) - sin(r_cLat) * cos(r_wLat) * cos(d_lon) as "X"
from rad_Calc)
--
,
--
qry_bearing_Calc as
--
(select t_site
, t_cell
, wntd_id
, case when X = 0 and Y = 0 then 0
      else cast ((degrees(atan2(Y,X)) + 360) as decimal(5,2)) % 360
  end as "S2U_Bearing"
from xy_Calc)
--
,
-- #### Antenna Azimuth ####
--
qry_Antenna as
--
(with
--
qry_antObj as 
--
(select * from
--
(select * 
, rank() over (partition by antenna_type, antenna_port order by Beam_offset is null, updated_by is null, updated_on desc) Ranked
from hive.thor_fw.v_cur_appian_gold_fw_siteconfig_ref_antennaconfig 
where row_status_id = 1)
where Ranked = 1)
------------------------------------ Bearing --------------------------------
,
--
qry_antenna as
--
(select s_conf.dt
, s_ref.site_name
, s_conf.pow_id "POW Key siteconfig_cellconfig"
, p_pow.pow_id "POW Key POW"
, p_pow.pow_text_id
, m_list_one.value "cell_id"
, eutran_cell_id
, m_list_two.value "antenna_id"
, azimuth_beam_azimuth
, "azimuth"
, m_list_four.value "beam_offset"
, m_list_three.value "antenna_type"	
, azimuth_beam_azimuth "azimuth_beam_azimuth DB"
, mod(mod (try_cast(if(m_list_four.value is null, '0', m_list_four.value) as Double)  + try_cast(azimuth as Double), 360) + 360 , 360) "cell_bearing"
, r_code.value "POW Site Status"
--	
  from hive.thor_fw.v_cur_appian_gold_fw_siteconfig_cellconfig s_conf 
--
left join hive.thor_fw.v_cur_appian_gold_fw_pow p_pow on p_pow.pow_id = s_conf.pow_id
left join hive.thor_fw.v_cur_appian_gold_fw_site s_info on s_info.pow_id  = p_pow.pow_id
left join hive.thor_fw.v_cur_appian_gold_fw_siteconfig_ref_sites s_ref on s_ref.site_ref_id = s_info.site_ref_id
left join hive.thor_fw.v_cur_appian_gold_fw_siteconfig_antennadetails anthe on anthe.pow_id = s_conf.pow_id and anthe.antenna_id = s_conf.antenna_id and anthe.row_status_id=1
left join qry_antObj on qry_antObj.antenna_type = anthe.antenna_type and qry_antObj.antenna_port=s_conf.antenna_port and qry_antObj.row_status_id=1
left join hive.thor_fw.v_cur_appian_gold_cmn_expref_managedlist m_list_one on m_list_one.managed_list_id = s_conf.cell_id
left join hive.thor_fw.v_cur_appian_gold_cmn_expref_managedlist m_list_two on m_list_two.managed_list_id = s_conf.antenna_id
left join hive.thor_fw.v_cur_appian_gold_cmn_expref_managedlist m_list_three on m_list_three.managed_list_id = anthe.antenna_type
left join hive.thor_fw.v_cur_appian_gold_cmn_expref_managedlist m_list_four on m_list_four.managed_list_id = qry_antObj.beam_offset
left join hive.thor_fw.v_cur_appian_gold_cmn_ref_code r_code on p_pow.site_status_id = r_code.code_id
where 1=1
and r_code.value like 'Latest')
--
select * from qry_antenna)
------------------------------------- Cell Tilts ----------------------------
,
--
qry_m_Tricks as
--
(select eutran_cell_id
, dt
, round(prb_util_ul_roll14,3) as "prb_util_ul_r14"
, round(prb_util_dl_roll14,3) as "prb_util_dl_r14"
from
hive.thor_fw.v_wireless_pm_enodeb_e_utran_cell_bh_local
where dt = cast(to_char(now() - interval '1' day,'yyyymmdd') as integer))
--
,
--
qry_antenna_table as
--
(select 'AIR3239_B40' as antenna_model, 65 as t_cell_beamwidth union
select 'AIR6419 2300MHz _B40' as antenna_model, 65 as t_cell_beamwidth union
select 'Lens_B40' as antenna_model, 13 as t_cell_beamwidth union
select 'LLPX210M_B40' as antenna_model, 30 as t_cell_beamwidth union
select 'LLPX210R_B40' as antenna_model, 30 as t_cell_beamwidth union
select 'LLPX210R-V1_B40' as antenna_model, 30 as t_cell_beamwidth union
select 'LLPX310F-0-V1_B40' as antenna_model, 60 as t_cell_beamwidth union
select 'LLPX310F-2-V1_B40' as antenna_model, 60 as t_cell_beamwidth union
select 'LLPX310F-6-V1_B40' as antenna_model, 60 as t_cell_beamwidth union
select 'LLPX310R_B40' as antenna_model, 60 as t_cell_beamwidth union
select 'LLPX310R-V1_B40' as antenna_model, 60 as t_cell_beamwidth union
select 'LLPX411F-0_B40' as antenna_model, 90 as t_cell_beamwidth union
select 'LLPX411F-2_B40' as antenna_model, 90 as t_cell_beamwidth union
select 'LLPX411F-6_B40' as antenna_model, 90 as t_cell_beamwidth union
select 'LLPX411R_B40' as antenna_model, 90 as t_cell_beamwidth union
select 'LLPX411R-V1_B40' as antenna_model, 90 as t_cell_beamwidth union
select 'AW3842_B40' as antenna_model, 65 as t_cell_beamwidth union
select 'AW3843_B40' as antenna_model, 33 as t_cell_beamwidth union
select 'AW3919-E-F_B40' as antenna_model, 65 as t_cell_beamwidth union
select 'DB 65 Amp 6186502E_B40' as antenna_model, 60 as t_cell_beamwidth union
select 'KP-23DOMNI-HV_B40' as antenna_model, 360 as t_cell_beamwidth union
select 'MINI LENS_B40' as antenna_model, 20 as t_cell_beamwidth union
select 'MINI LENS-HP_B40' as antenna_model, 17 as t_cell_beamwidth union
select 'Mini Lens-HP-MQ4_B40' as antenna_model, 17 as t_cell_beamwidth union
select 'Mini Lens-HP-MQ4_B42' as antenna_model, 13 as t_cell_beamwidth union
select 'AIR3219 3400MHz_B42' as antenna_model, 65 as t_cell_beamwidth union
select 'AIR6488_B42' as antenna_model, 60 as t_cell_beamwidth union
select 'AW3497_B42' as antenna_model, 30 as t_cell_beamwidth union
select 'AW3497-C_B42' as antenna_model, 30 as t_cell_beamwidth union
select 'AW3711_B42' as antenna_model, 30 as t_cell_beamwidth union
select 'AW3842_B42' as antenna_model, 65 as t_cell_beamwidth union
select 'AW3843_B42' as antenna_model, 33 as t_cell_beamwidth union
select 'AW3919-E-F_B42' as antenna_model, 65 as t_cell_beamwidth union
select 'DB 65 Amp 6186502E_B42' as antenna_model, 60 as t_cell_beamwidth union
select 'KP-23DOMNI-HV_B42' as antenna_model, 360 as t_cell_beamwidth union
select 'Lens Wideband_B42' as antenna_model, 13 as t_cell_beamwidth union
select 'MINI LENS_B42' as antenna_model, 13 as t_cell_beamwidth union
select 'MINI LENS-HP_B42' as antenna_model, 13 as t_cell_beamwidth union
select 'SSPX310R_B42' as antenna_model, 60 as t_cell_beamwidth union
select 'SSPX310R-V2_B42' as antenna_model, 60 as t_cell_beamwidth union
select 'AIR5322_N257' as antenna_model, 100 as t_cell_beamwidth union
select 'Mini Lens-12 Beam_B42' as antenna_model, 12.3 as t_cell_beamwidth)
--
--
-- $$$$ Bingo Royalties "should results need to be verifyed, add column main_input.reference_value..."
--
--
select main_input.newest_m3_date
, main_input.wntd_version
, main_input.wntd_id
, main_input.imsi
, main_input.location_id
, main_input.avc_id
, main_input.s_cell
, main_input.s_site
, main_input.pci
, main_input.rsrp
, main_input.cinr
, main_input.rsrq
, main_input.earfcn
, main_input.reference_value
, main_input.channel_flag
, main_input.poi_site_code
, main_input.cluster_name
, main_input.AVC_UL_GB
, main_input.AVC_DL_GB
, main_input.gps_location_latitude
, main_input.gps_location_longitude
, main_input.t_cell
, main_input.connected_devices
, qry_m_Tricks.prb_util_ul_r14
, qry_m_Tricks.prb_util_dl_r14
, main_input.antenna_type
, main_input.t_site
, main_input.t_lat
, main_input.t_lon
, main_input.U2S_Distance_Km 
, main_input.distance_rank
, main_input.t_cell_check
, qry_Antenna.cell_bearing
, qry_Antenna_table.t_cell_beamwidth
, case when (qry_Antenna.cell_bearing - (qry_Antenna_table.t_cell_beamwidth * 0.5)) < 0 
       then (360 + (qry_Antenna.cell_bearing - (qry_Antenna_table.t_cell_beamwidth * 0.5))) 
       else (qry_Antenna.cell_bearing - (qry_Antenna_table.t_cell_beamwidth * 0.5)) end as "cell_edge_low"
--
, qry_b.S2U_Bearing
--       
, case when (qry_Antenna.cell_bearing + (qry_Antenna_table.t_cell_beamwidth * 0.5)) > 360 
       then ((qry_Antenna.cell_bearing + (qry_Antenna_table.t_cell_beamwidth * 0.5)) - 360) 
       else (qry_Antenna.cell_bearing + (qry_Antenna_table.t_cell_beamwidth * 0.5)) end as "cell_edge_high"       
--
from main_input
--
inner join qry_bearing_Calc qry_b on main_input.wntd_id = qry_b.wntd_id 
and main_input.t_cell = qry_b.t_cell
--
inner join qry_antenna on qry_b.t_cell = qry_antenna.eutran_cell_id
inner join qry_m_Tricks on main_input.t_cell = qry_m_Tricks.eutran_cell_id 
inner join qry_antenna_table on replace(main_input.antenna_cell_band, ' ', '') = replace(qry_antenna_table.antenna_model, ' ', '') 
--
group by main_input.newest_m3_date
, main_input.wntd_version
, main_input.wntd_id
, main_input.imsi
, main_input.location_id
, main_input.avc_id
, main_input.s_cell
, main_input.s_site
, main_input.pci
, main_input.rsrp
, main_input.cinr
, main_input.rsrq
, main_input.earfcn
, main_input.channel_flag
, main_input.reference_value
, main_input.poi_site_code
, main_input.cluster_name
, main_input.AVC_UL_GB
, main_input.AVC_DL_GB
, main_input.gps_location_latitude
, main_input.gps_location_longitude
, main_input.t_cell
, main_input.connected_devices
, qry_m_Tricks.prb_util_ul_r14
, qry_m_Tricks.prb_util_dl_r14
, main_input.antenna_type
, main_input.t_site
, main_input.t_lat
, main_input.t_lon
, main_input.U2S_Distance_Km 
, main_input.distance_rank
, main_input.t_cell_check
, qry_antenna.cell_bearing
, qry_antenna_table.t_cell_beamwidth
, qry_b.S2U_Bearing)
--
-- #### Align registries for S2T delta calculations
--
select mdf_1.newest_m3_date
, mdf_1.wntd_version
, mdf_1.wntd_id
, mdf_1.imsi
, mdf_1.location_id
, mdf_1.s_site
, mdf_2.t_site
, mdf_1.s_cell
, mdf_2.t_cell
, mdf_1.s_pci
, mdf_2.t_pci
, mdf_1.s_rsrp
, mdf_2.t_rsrp
, mdf_1.s_cinr
, mdf_2.t_cinr
, mdf_1.s_rsrq
, mdf_2.t_rsrq
, mdf_1.s_earfcn
, mdf_2.t_earfcn
, mdf_1.s_channel_flag
, mdf_2.t_channel_flag
, mdf_1.s_connected_devices
, mdf_2.t_connected_devices
, mdf_1.s_prb_util_ul_r14
, mdf_2.t_prb_util_ul_r14
, mdf_1.s_prb_util_dl_r14
, mdf_2.t_prb_util_dl_r14
, mdf_1.s_antenna_type
, mdf_2.t_antenna_type
, mdf_1.s_cell_bearing
, mdf_2.t_cell_bearing
, mdf_1.s_cell_beamwidth
, mdf_2.t_cell_beamwidth
, mdf_1.s_cell_edge_low
, mdf_1.s_S2U_Bearing
, mdf_1.s_cell_edge_high
, mdf_2.t_cell_edge_low
, mdf_2.t_S2U_Bearing
, mdf_2.t_cell_edge_high
, mdf_1.s_U2S_Distance_Km
, mdf_2.t_U2S_Distance_Km
, try_cast(mdf_1.s_rsrp as double) - try_cast(mdf_2.t_rsrp as double) as "s2t_rsrp_delta"
, try_cast(mdf_1.s_rsrq as double) - try_cast(mdf_2.t_rsrq as double) as "s2t_rsrq_delta"
, try_cast(mdf_1.s_connected_devices as integer) - try_cast(mdf_2.t_connected_devices as integer)  as "s2t_connected_devices_delta"
, try_cast(mdf_1.s_prb_util_ul_r14 as double) - try_cast(mdf_2.t_prb_util_ul_r14 as double) as "s2t_prb_util_ul_delta"
, try_cast(mdf_1.s_prb_util_dl_r14 as double) - try_cast(mdf_2.t_prb_util_dl_r14 as double) as "s2t_prb_util_dl_delta"
, case when (mdf_1.s_S2U_Bearing < mdf_1.s_cell_edge_low and mdf_1.s_S2U_Bearing > mdf_1.s_cell_edge_high) then '0'
       when (mdf_1.s_S2U_Bearing > mdf_1.s_cell_edge_low and mdf_1.s_S2U_Bearing < mdf_1.s_cell_edge_high) then '1'
       when (mdf_1.s_S2U_Bearing < mdf_1.s_cell_edge_low and mdf_1.s_S2U_Bearing < mdf_1.s_cell_edge_high) then '0' 
       when (mdf_1.s_S2U_Bearing > mdf_1.s_cell_edge_low and mdf_1.s_S2U_Bearing > mdf_1.s_cell_edge_high) then '0' end as "s_cell_S2U_Bearing_confirmed"
, case when (mdf_2.t_S2U_Bearing < mdf_2.t_cell_edge_low and mdf_2.t_S2U_Bearing > mdf_2.t_cell_edge_high) then '0' 
       when (mdf_2.t_S2U_Bearing > mdf_2.t_cell_edge_low and mdf_2.t_S2U_Bearing < mdf_2.t_cell_edge_high) then '1' 
       when (mdf_2.t_S2U_Bearing < mdf_2.t_cell_edge_low and mdf_2.t_S2U_Bearing < mdf_2.t_cell_edge_high) then '0'
       when (mdf_2.t_S2U_Bearing > mdf_2.t_cell_edge_low and mdf_2.t_S2U_Bearing > mdf_2.t_cell_edge_high) then '0' end as "t_cell_S2U_Bearing_confirmed"
, case when (mdf_1.s_channel_flag > 1 or mdf_2.t_channel_flag > 1) then 'co_channel' else 'clean_channel' end as "Channel_Status"
--
from
--
(select mod_dataFrame.newest_m3_date
, mod_dataFrame.wntd_version
, mod_dataFrame.wntd_id
, mod_dataFrame.imsi
, mod_dataFrame.location_id
, mod_dataFrame.s_cell
, mod_dataFrame.s_site
, mod_dataFrame.pci as "s_pci"
, mod_dataFrame.rsrp as "s_rsrp"
, mod_dataFrame.cinr as "s_cinr"
, mod_dataFrame.rsrq as "s_rsrq"
, mod_dataFrame.earfcn as "s_earfcn"
, mod_dataFrame.channel_flag as "s_channel_flag"
, mod_dataFrame.connected_devices "s_connected_devices"
, mod_dataFrame.prb_util_ul_r14 as "s_prb_util_ul_r14"
, mod_dataFrame.prb_util_dl_r14 as "s_prb_util_dl_r14"
, mod_dataFrame.U2S_Distance_Km as "s_U2S_Distance_Km"
, mod_dataFrame.antenna_type as "s_antenna_type"
, mod_dataFrame.cell_bearing as "s_cell_bearing"
, mod_dataFrame.t_cell_beamwidth as "s_cell_beamwidth"
, mod_dataFrame.cell_edge_low as "s_cell_edge_low"
, mod_dataFrame.S2U_Bearing as "s_S2U_Bearing"
, mod_dataFrame.cell_edge_high as "s_cell_edge_high"
--
from mod_dataFrame
--
where mod_dataFrame.t_cell_check = 1) mdf_1
--
inner join
--
(select mod_dataFrame.newest_m3_date
, mod_dataFrame.wntd_version
, mod_dataFrame.wntd_id
, mod_dataFrame.imsi
, mod_dataFrame.location_id
, mod_dataFrame.s_cell
, mod_dataFrame.s_site
, mod_dataFrame.pci as "t_pci"
, mod_dataFrame.rsrp as "t_rsrp"
, mod_dataFrame.cinr as "t_cinr"
, mod_dataFrame.rsrq as "t_rsrq"
, mod_dataFrame.earfcn as "t_earfcn"
, mod_dataFrame.channel_flag as "t_channel_flag"
, mod_dataFrame.t_site
, mod_dataFrame.t_cell
, mod_dataFrame.connected_devices "t_connected_devices"
, mod_dataFrame.prb_util_ul_r14 as "t_prb_util_ul_r14"
, mod_dataFrame.prb_util_dl_r14 as "t_prb_util_dl_r14"
, mod_dataFrame.U2S_Distance_Km as "t_U2S_Distance_Km"
, mod_dataFrame.antenna_type as "t_antenna_type"
, mod_dataFrame.cell_bearing as "t_cell_bearing"
, mod_dataFrame.t_cell_beamwidth as "t_cell_beamwidth"
, mod_dataFrame.cell_edge_low as "t_cell_edge_low"
, mod_dataFrame.S2U_Bearing as "t_S2U_Bearing"
, mod_dataFrame.cell_edge_high as "t_cell_edge_high"
--
from mod_dataFrame
--
where mod_dataFrame.t_cell_check = 0) mdf_2
--
on mdf_1.newest_m3_date = mdf_2.newest_m3_date
and mdf_1.imsi = mdf_2.imsi
and mdf_1.location_id = mdf_2.location_id
and mdf_1.s_cell <> mdf_2.t_cell
-- end"""