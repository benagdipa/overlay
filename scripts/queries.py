active_avc_query = """select location_id
from hive.thor_fw.v_cur_eps_inventory_installed_wntd
where total_active_avc > 0"""

eps_query = """select location_id, wntd_version, wntd_id, imsi, eutran_cell_id "s_cell", site_name "s_site"
, case when C.latitude is null then gps_location_latitude else C.latitude end "loc_lat"
, case when C.longitude is null then gps_location_longitude else C.longitude end "loc_long"
, substring(eutran_cell_id,12,4) as "EPS site code"
, concat(site_name,'_',case when substring(eutran_cell_id,18,1)='0' then try_cast(try_cast(substring(eutran_cell_id,20,2) as integer) as varchar)  else try_cast(try_cast(substring(eutran_cell_id,20,2) as integer)+100 as varchar) end) "s_cell_name"
from hive.thor_fw.v_cur_eps_inventory_installed_wntd A
left join hive_user_upload.thor_user_files.ahmedbehiry_nsoc_site_name_mapping B
on substring(A.eutran_cell_id,1,15) = B.nsoc_site_code
left join hive.thor_common.v_cur_hfl_loc C
on A.location_id = C.nbn_location_id"""

user_count_query = """select eutran_cell_id "s_cell",  count(*) "s_connected_devices"
, concat(site_name,'_',case when substring(eutran_cell_id,18,1)='0' then try_cast(try_cast(substring(eutran_cell_id,20,2) as integer) as varchar)  else try_cast(try_cast(substring(eutran_cell_id,20,2) as integer)+100 as varchar) end) "s_cell_name"
from hive.thor_fw.v_cur_eps_inventory_installed_wntd A
left join hive_user_upload.thor_user_files.ahmedbehiry_nsoc_site_name_mapping B
on substring(A.eutran_cell_id,1,15) = B.nsoc_site_code
group by eutran_cell_id, site_name"""

production_query = """select e_utran_cell "s_cell", pci "s_pci" , earfcn "s_earfcn", eci "Current ECI"
, concat(site_name, '_', try_cast(case when substring(e_utran_cell,18,1) = '0' then try_cast(substring(e_utran_cell,20,2) as integer) else try_cast(substring(e_utran_cell,20,2) as integer) + 100 end as varchar)) "s_cell_name"
from hive.thor_fw.v_cur_enm_ran_topology_e_utran_cell_tdd A
left join hive_user_upload.thor_user_files.ahmedbehiry_nsoc_site_name_mapping B
on substring(A.e_utran_cell,1,15) = B.nsoc_site_code"""

bh_kpis_query = """select eutran_cell_id
, concat(site_name,'_',cast(C.cell_id as varchar)) "s_cell_name"
, avg(prb_util_dl) "s_prb_util_dl_r14", avg(prb_util_ul) "s_prb_util_ul_r14"
from hive.thor_fw.v_wireless_pm_enodeb_e_utran_cell_bh_local A

left join hive_user_upload.thor_user_files.ahmedbehiry_nsoc_site_name_mapping B
on substring(A.eutran_cell_id,1,15) = B.nsoc_site_code

left join hive.thor_fw.v_cur_enm_ran_topology_e_utran_cell_tdd C
on A.eutran_cell_id = C.e_utran_cell 

where A.dt >= cast(date_format(current_date - interval '14' day, '%Y%m%d') as integer) and C.administrative_state like 'UNLOCKED'
group by A.eutran_cell_id, B.site_name, C.cell_id"""

ENM_query_part1 = """ 
        select e_utran_cell as "t_cell",
        site_name as "t_site",
        substring(e_utran_cell,12,4) as "Scanned Site Code",
        pci,
        earfcn
        from hive.thor_fw.v_cur_enm_ran_topology_e_utran_cell_tdd A
        left join hive_user_upload.thor_user_files.ahmedbehiry_nsoc_site_name_mapping B
        on substring(A.e_utran_cell,1,15) = B.nsoc_site_code
        where substring(e_utran_cell,12,4) in ("""
ENM_query_part2 = """)"""

cc_query = """select eutran_cell_id "s_cell"
, concat(c.site_name,'_',cast(b.cell_id as varchar)) "cell_name"
, case when sum(pm_ca_configured_dl_sum_1) is null and sum(pm_ca_configured_dl_sum_2) is null and sum(pm_ca_configured_dl_sum_3) is null and sum(pm_ca_configured_dl_sum_4) is null then null 
	   when sum(pm_ca_configured_dl_sum_4)>0 then 4
	   when sum(pm_ca_configured_dl_sum_3)>0 then 3
	   when sum(pm_ca_configured_dl_sum_2)>0 then 2
	   else 1 end "s_dl_cc"
, case when sum(pm_ca_configured_ul_sum_1) is null and sum(pm_ca_configured_ul_sum_2) is null then null
	   when sum(pm_ca_configured_ul_sum_2)>0 then 2
	   else 1 end "s_ul_cc"
from hive.thor_fw.v_wireless_pm_enodeb_e_utran_cell_hourly_local a

left join hive.thor_fw.v_cur_enm_ran_topology_e_utran_cell_tdd b
on a.eutran_cell_id = b.e_utran_cell 

left join hive_user_upload.thor_user_files.ahmedbehiry_nsoc_site_name_mapping c
on substring(a.eutran_cell_id,1,15) = c.nsoc_site_code 

where a.dt >= cast(date_format(current_date - interval '7' day, '%Y%m%d') as integer) and b.administrative_state like 'UNLOCKED'
group by a.eutran_cell_id, c.site_name, b.cell_id"""

coordinates_query = """ 
        select substring(nsoc_site_code,12,4) as Code,
        actual_latitude,
        actual_longitude
        from hive.thor_fw.v_cur_appian_cell_enrichment_all
        where nsoc_site_code is not null and actual_latitude is not null and actual_longitude is not null
        group by nsoc_site_code, actual_latitude, actual_longitude"""

appian_query = """select eutran_cell_id
, cell_id
, site_name "s_site"
, concat(site_name,'_',try_cast(cell_id as varchar)) "s_cell_name"
, actual_latitude "s_lat"
, actual_longitude "s_long"
, (case when MultiBeam_Bearing is not null then MultiBeam_Bearing else try_cast(azimuth as double) end) as "s_cell_bearing"
, height
, antenna_type "s_antenna_type"
, case when substring(antenna_type,1,7) = 'LLPX210' then '30' else
	case when substring(antenna_type,1,7) = 'LLPX411' then '90' else
	 case when substring(antenna_type,1,7) = 'LLPX310' then '60' else
	  case when antenna_type = 'AW3842' then '65' else
	   case when antenna_type = 'AW3843' then '33' else
	    case when substring(antenna_type,1, 2) = 'AW' then '30' else
	     case when antenna_type = 'DB 65 Amp 6186502E' then '60' else
	      case when antenna_type = 'Lens' then '13' else
	       case when antenna_type = 'Lens Wideband' then '13' else
	        case when antenna_type = 'MINI LENS' then '20' else
	         case when antenna_type = 'MINI LENS-HP' then '17' else
	          case when antenna_type = 'Mini Lens-HP-MQ4' then '17' else
	           case when antenna_type = 'Mini Lens-12 Beam' then '10' else
	            case when substring(antenna_type,1,7) = 'SSPX310' then '60' else
	             case when substring(antenna_type,1,3) = 'AIR' then '65' else '' end end end end end end end end end end end end end end end as "s_cell_beamwidth"
--
from
--
(select a_db.eutran_cell_id
, a_db.site_name
, a_db.cell_id
, a_db.actual_latitude
, a_db.actual_longitude
, a_db.height
, a_db.azimuth
, a_db.antenna_type
--
, (case when least(coalesce(Lens_Beam1,infinity()),
coalesce(Lens_Beam2,infinity()),
coalesce(Lens_Beam3,infinity()),
coalesce(Lens_Beam4,infinity()),
coalesce(Lens_Beam5,infinity()),
coalesce(Lens_Beam6,infinity()),
coalesce(Lens_Beam7,infinity()),
coalesce(Lens_Beam8,infinity()),
coalesce(Lens_Beam9,infinity()),
coalesce(MN_L_Beam1,infinity()),
coalesce(MN_L_Beam2,infinity()),
coalesce(MN_L_Beam3,infinity()),
coalesce(MN_L_Beam4,infinity()),
coalesce(MN_L_Beam5,infinity()),
coalesce(MN_L_Beam6,infinity()),
coalesce(MN_L_Beam7,infinity()),
coalesce(MN_L_Beam8,infinity()),
coalesce(MN_L_HP_Beam1,infinity()),
coalesce(MN_L_HP_Beam2,infinity()),
coalesce(MN_L_HP_Beam3,infinity()),
coalesce(MN_L_HP_Beam4,infinity()),
coalesce(MN_L_HP_Beam5,infinity()),
coalesce(MN_L_HP_Beam6,infinity()),
coalesce(MN_L_HP_Beam7,infinity()),
coalesce(MN_L_HP_Beam8,infinity()),
coalesce(MN_L_HP_MQ4_Beam1,infinity()),
coalesce(MN_L_HP_MQ4_Beam2,infinity()),
coalesce(MN_L_HP_MQ4_Beam3,infinity()),
coalesce(MN_L_HP_MQ4_Beam4,infinity()),
coalesce(MN_L_HP_MQ4_Beam5,infinity()),
coalesce(MN_L_HP_MQ4_Beam6,infinity()),
coalesce(MN_L_HP_MQ4_Beam7,infinity()),
coalesce(MN_L_HP_MQ4_Beam8,infinity()),
coalesce(MN_L_12_Beams_Beam1,infinity()),
coalesce(MN_L_12_Beams_Beam2,infinity()),
coalesce(MN_L_12_Beams_Beam3,infinity()),
coalesce(MN_L_12_Beams_Beam4,infinity()),
coalesce(MN_L_12_Beams_Beam5,infinity()),
coalesce(MN_L_12_Beams_Beam6,infinity()),
coalesce(MN_L_12_Beams_Beam7,infinity()),
coalesce(MN_L_12_Beams_Beam8,infinity()),
coalesce(MN_L_12_Beams_Beam9,infinity()),
coalesce(MN_L_12_Beams_Beam10,infinity()),
coalesce(MN_L_12_Beams_Beam11,infinity()),
coalesce(MN_L_12_Beams_Beam12,infinity())
--
) <> infinity()
--
then least(coalesce(Lens_Beam1,infinity()),
coalesce(Lens_Beam2,infinity()),
coalesce(Lens_Beam3,infinity()),
coalesce(Lens_Beam4,infinity()),
coalesce(Lens_Beam5,infinity()),
coalesce(Lens_Beam6,infinity()),
coalesce(Lens_Beam7,infinity()),
coalesce(Lens_Beam8,infinity()),
coalesce(Lens_Beam9,infinity()),
coalesce(MN_L_Beam1,infinity()),
coalesce(MN_L_Beam2,infinity()),
coalesce(MN_L_Beam3,infinity()),
coalesce(MN_L_Beam4,infinity()),
coalesce(MN_L_Beam5,infinity()),
coalesce(MN_L_Beam6,infinity()),
coalesce(MN_L_Beam7,infinity()),
coalesce(MN_L_Beam8,infinity()),
coalesce(MN_L_HP_Beam1,infinity()),
coalesce(MN_L_HP_Beam2,infinity()),
coalesce(MN_L_HP_Beam3,infinity()),
coalesce(MN_L_HP_Beam4,infinity()),
coalesce(MN_L_HP_Beam5,infinity()),
coalesce(MN_L_HP_Beam6,infinity()),
coalesce(MN_L_HP_Beam7,infinity()),
coalesce(MN_L_HP_Beam8,infinity()),
coalesce(MN_L_HP_MQ4_Beam1,infinity()),
coalesce(MN_L_HP_MQ4_Beam2,infinity()),
coalesce(MN_L_HP_MQ4_Beam3,infinity()),
coalesce(MN_L_HP_MQ4_Beam4,infinity()),
coalesce(MN_L_HP_MQ4_Beam5,infinity()),
coalesce(MN_L_HP_MQ4_Beam6,infinity()),
coalesce(MN_L_HP_MQ4_Beam7,infinity()),
coalesce(MN_L_HP_MQ4_Beam8,infinity()),
coalesce(MN_L_12_Beams_Beam1,infinity()),
coalesce(MN_L_12_Beams_Beam2,infinity()),
coalesce(MN_L_12_Beams_Beam3,infinity()),
coalesce(MN_L_12_Beams_Beam4,infinity()),
coalesce(MN_L_12_Beams_Beam5,infinity()),
coalesce(MN_L_12_Beams_Beam6,infinity()),
coalesce(MN_L_12_Beams_Beam7,infinity()),
coalesce(MN_L_12_Beams_Beam8,infinity()),
coalesce(MN_L_12_Beams_Beam9,infinity()),
coalesce(MN_L_12_Beams_Beam10,infinity()),
coalesce(MN_L_12_Beams_Beam11,infinity()),
coalesce(MN_L_12_Beams_Beam12,infinity())
)
else null
end ) as "MultiBeam_Bearing"
--
from
--
(select eutran_cell_id
, cell_id
, site_name
, actual_latitude
, actual_longitude
, height
, azimuth
, antenna_type
--
,(case when antenna_type like 'Lens%' and map1_ports like '1+2' then
(case when try_cast(azimuth as double)-53.3 <0 then 360-(53.3-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-53.3 >=360 then 360-(try_cast(azimuth as double)-53.3)
else try_cast(azimuth as double)-53.3 end)
end)
end ) as "Lens_Beam1"
--
,(case when antenna_type like 'Lens%' and map1_ports like '3+4' then
(case when try_cast(azimuth as double)-40 <0 then 360-(40-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-40 >=360 then 360-(try_cast(azimuth as double)-40)
else try_cast(azimuth as double)-40 end)
end)
end ) as "Lens_Beam2"
--
,(case when antenna_type like 'Lens%' and map1_ports like '5+6' then 
(case when try_cast(azimuth as double)-26.7 <0 then 360-(26.7-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-26.7 >=360 then 360-(try_cast(azimuth as double)-26.7)
else try_cast(azimuth as double)-26.7 end)
end)
end ) as "Lens_Beam3"
--
,(case when antenna_type like 'Lens%' and map1_ports like '7+8' then 
(case when try_cast(azimuth as double)-13.3 <0 then 360-(13.3-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-13.3 >=360 then 360-(try_cast(azimuth as double)-13.3)
else try_cast(azimuth as double)-13.3 end)
end)
end ) as "Lens_Beam4"
--
,(case when antenna_type like 'Lens%' and map1_ports like '9+10' then try_cast(azimuth as double) end )
as "Lens_Beam5"
--
,(case when antenna_type like 'Lens%' and map1_ports like '11+12' then
(case when try_cast(azimuth as double)+13.3 >=360 then (try_cast(azimuth as double)+13.3)-360
else try_cast(azimuth as double)+13.3 end)
end ) as "Lens_Beam6"
--
,(case when antenna_type like 'Lens%' and map1_ports like '13+14' then
(case when try_cast(azimuth as double)+26.7 >=360 then (try_cast(azimuth as double)+26.7)-360
else try_cast(azimuth as double)+26.7 end)
end ) as "Lens_Beam7"
--
,(case when antenna_type like 'Lens%' and map1_ports like '15+16' then
(case when try_cast(azimuth as double)+40 >=360 then (try_cast(azimuth as double)+40)-360
else try_cast(azimuth as double)+40 end)
end ) as "Lens_Beam8"
--
,(case when antenna_type like 'Lens%' and map1_ports like '17+18' then 
(case when try_cast(azimuth as double)+53.3 >=360 then (try_cast(azimuth as double)+53.3)-360
else try_cast(azimuth as double)+53.3 end)
end ) as "Lens_Beam9"
--
,(case when antenna_type like 'MINI LENS' and map1_ports like '1+2' then
(case when try_cast(azimuth as double)-52 <0 then 360-(52-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-52 >=360 then 360-(try_cast(azimuth as double)-52)
else try_cast(azimuth as double)-52 end)
end)
end ) as "MN_L_Beam1"
--
,(case when antenna_type like 'MINI LENS' and map1_ports like '3+4' then
(case when try_cast(azimuth as double)-37 <0 then 360-(37-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-37 >=360 then 360-(try_cast(azimuth as double)-37)
else try_cast(azimuth as double)-37 end)
end)
end ) as "MN_L_Beam2"
--
,(case when antenna_type like 'MINI LENS' and map1_ports like '5+6' then
(case when try_cast(azimuth as double)-22 <0 then 360-(22-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-22 >=360 then 360-(try_cast(azimuth as double)-22)
else try_cast(azimuth as double)-22 end)
end)
end ) as "MN_L_Beam3"
--
,(case when antenna_type like 'MINI LENS' and map1_ports like '7+8' then
(case when try_cast(azimuth as double)-7 <0 then 360-(7-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-7 >=360 then 360-(try_cast(azimuth as double)-7)
else try_cast(azimuth as double)-7 end)
end)
end ) as "MN_L_Beam4"
--
,(case when antenna_type like 'MINI LENS' and map1_ports like '9+10' then
(case when try_cast(azimuth as double)+8 >=360 then (try_cast(azimuth as double)+8)-360
else try_cast(azimuth as double)+8 end)
end ) as "MN_L_Beam5"
--
,(case when antenna_type like 'MINI LENS' and map1_ports like '11+12' then
(case when try_cast(azimuth as double)+23 >=360 then (try_cast(azimuth as double)+23)-360
else try_cast(azimuth as double)+23 end)
end ) as "MN_L_Beam6"
--
,(case when antenna_type like 'MINI LENS' and map1_ports like '13+14' then
(case when try_cast(azimuth as double)+38 >=360 then (try_cast(azimuth as double)+38)-360
else try_cast(azimuth as double)+38 end)
end ) as "MN_L_Beam7"
--
,(case when antenna_type like 'MINI LENS' and map1_ports like '15+16' then
(case when try_cast(azimuth as double)+53 >=360 then (try_cast(azimuth as double)+53)-360
else try_cast(azimuth as double)+53 end)
end ) as "MN_L_Beam8"
--
,(case when antenna_type like 'MINI LENS-HP' and map1_ports like '1+2' then
(case when try_cast(azimuth as double)-52 <0 then 360-(52-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-52 >=360 then 360-(try_cast(azimuth as double)-52)
else try_cast(azimuth as double)-52 end)
end)
end ) as "MN_L_HP_Beam1"
--
,(case when antenna_type like 'MINI LENS-HP' and map1_ports like '3+4' then
(case when try_cast(azimuth as double)-37 <0 then 360-(37-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-37 >=360 then 360-(try_cast(azimuth as double)-37)
else try_cast(azimuth as double)-37 end)
end)
end ) as "MN_L_HP_Beam2"
--
,(case when antenna_type like 'MINI LENS-HP' and map1_ports like '5+6' then
(case when try_cast(azimuth as double)-22 <0 then 360-(22-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-22 >=360 then 360-(try_cast(azimuth as double)-22)
else try_cast(azimuth as double)-22 end)
end)
end ) as "MN_L_HP_Beam3"
--
,(case when antenna_type like 'MINI LENS-HP' and map1_ports like '7+8' then
(case when try_cast(azimuth as double)-7 <0 then 360-(7-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-7 >=360 then 360-(try_cast(azimuth as double)-7)
else try_cast(azimuth as double)-7 end)
end)
end ) as "MN_L_HP_Beam4"
--
,(case when antenna_type like 'MINI LENS-HP' and map1_ports like '9+10' then
(case when try_cast(azimuth as double)+8 >=360 then (try_cast(azimuth as double)+8)-360
else try_cast(azimuth as double)+8 end)
end ) as "MN_L_HP_Beam5"
--
,(case when antenna_type like 'MINI LENS-HP' and map1_ports like '11+12' then
(case when try_cast(azimuth as double)+23 >=360 then (try_cast(azimuth as double)+23)-360
else try_cast(azimuth as double)+23 end)
end ) as "MN_L_HP_Beam6"
--
,(case when antenna_type like 'MINI LENS-HP' and map1_ports like '13+14' then
(case when try_cast(azimuth as double)+38 >=360 then (try_cast(azimuth as double)+38)-360
else try_cast(azimuth as double)+38 end)
end ) as "MN_L_HP_Beam7"
--
,(case when antenna_type like 'MINI LENS-HP' and map1_ports like '15+16' then
(case when try_cast(azimuth as double)+53 >=360 then (try_cast(azimuth as double)+53)-360
else try_cast(azimuth as double)+53 end)
end ) as "MN_L_HP_Beam8"
--
,(case when antenna_type like 'Mini Lens-HP-MQ4' and map1_ports like '1+2' then
(case when try_cast(azimuth as double)-52 <0 then 360-(52-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-52 >=360 then 360-(try_cast(azimuth as double)-52)
else try_cast(azimuth as double)-52 end)
end)
end ) as "MN_L_HP_MQ4_Beam1"
--
,(case when antenna_type like 'Mini Lens-HP-MQ4' and map1_ports like '3+4' then
(case when try_cast(azimuth as double)-37 <0 then 360-(37-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-37 >=360 then 360-(try_cast(azimuth as double)-37)
else try_cast(azimuth as double)-37 end)
end)
end ) as "MN_L_HP_MQ4_Beam2"
--
,(case when antenna_type like 'Mini Lens-HP-MQ4' and map1_ports like '5+6' then
(case when try_cast(azimuth as double)-22 <0 then 360-(22-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-22 >=360 then 360-(try_cast(azimuth as double)-22)
else try_cast(azimuth as double)-22 end)
end)
end ) as "MN_L_HP_MQ4_Beam3"
--
,(case when antenna_type like 'Mini Lens-HP-MQ4' and map1_ports like '7+8' then
(case when try_cast(azimuth as double)-7 <0 then 360-(7-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-7 >=360 then 360-(try_cast(azimuth as double)-7)
else try_cast(azimuth as double)-7 end)
end)
end ) as "MN_L_HP_MQ4_Beam4"
--
,(case when antenna_type like 'Mini Lens-HP-MQ4' and map1_ports like '9+10' then
(case when try_cast(azimuth as double)+8 >=360 then (try_cast(azimuth as double)+8)-360
else try_cast(azimuth as double)+8 end)
end ) as "MN_L_HP_MQ4_Beam5"
--
,(case when antenna_type like 'Mini Lens-HP-MQ4' and map1_ports like '11+12' then
(case when try_cast(azimuth as double)+23 >=360 then (try_cast(azimuth as double)+23)-360
else try_cast(azimuth as double)+23 end)
end ) as "MN_L_HP_MQ4_Beam6"
--
,(case when antenna_type like 'Mini Lens-HP-MQ4' and map1_ports like '13+14' then
(case when try_cast(azimuth as double)+38 >=360 then (try_cast(azimuth as double)+38)-360
else try_cast(azimuth as double)+38 end)
end ) as "MN_L_HP_MQ4_Beam7"
--
,(case when antenna_type like 'Mini Lens-HP-MQ4' and map1_ports like '15+16' then
(case when try_cast(azimuth as double)+53 >=360 then (try_cast(azimuth as double)+53)-360
else try_cast(azimuth as double)+53 end)
end ) as "MN_L_HP_MQ4_Beam8"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '1+2' then
(case when try_cast(azimuth as double)-55 <0 then 360-(55-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-55 >=360 then 360-(try_cast(azimuth as double)-55)
else try_cast(azimuth as double)-55 end)
end)
end ) as "MN_L_12_Beams_Beam1"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '3+4' then
(case when try_cast(azimuth as double)-45 <0 then 360-(45-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-45 >=360 then 360-(try_cast(azimuth as double)-45)
else try_cast(azimuth as double)-45 end)
end)
end ) as "MN_L_12_Beams_Beam2"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '5+6' then
(case when try_cast(azimuth as double)-35 <0 then 360-(35-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-35 >=360 then 360-(try_cast(azimuth as double)-35)
else try_cast(azimuth as double)-35 end)
end)
end ) as "MN_L_12_Beams_Beam3"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '7+8' then
(case when try_cast(azimuth as double)-25 <0 then 360-(25-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-25 >=360 then 360-(try_cast(azimuth as double)-25)
else try_cast(azimuth as double)-25 end)
end)
end ) as "MN_L_12_Beams_Beam4"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '9+10' then
(case when try_cast(azimuth as double)-15 <0 then 360-(15-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-15 >=360 then 360-(try_cast(azimuth as double)-15)
else try_cast(azimuth as double)-15 end)
end)
end ) as "MN_L_12_Beams_Beam5"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '11+12' then
(case when try_cast(azimuth as double)-5 <0 then 360-(5-try_cast(azimuth as double)) else 
(case when try_cast(azimuth as double)-5 >=360 then 360-(try_cast(azimuth as double)-5)
else try_cast(azimuth as double)-5 end)
end)
end ) as "MN_L_12_Beams_Beam6"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '13+14' then
(case when try_cast(azimuth as double)+5 >=360 then (try_cast(azimuth as double)+5)-360
else try_cast(azimuth as double)+5 end)
end ) as "MN_L_12_Beams_Beam7"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '15+16' then
(case when try_cast(azimuth as double)+15 >=360 then (try_cast(azimuth as double)+15)-360
else try_cast(azimuth as double)+15 end)
end ) as "MN_L_12_Beams_Beam8"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '17+18' then
(case when try_cast(azimuth as double)+25 >=360 then (try_cast(azimuth as double)+25)-360
else try_cast(azimuth as double)+25 end)
end ) as "MN_L_12_Beams_Beam9"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '19+20' then
(case when try_cast(azimuth as double)+35 >=360 then (try_cast(azimuth as double)+35)-360
else try_cast(azimuth as double)+35 end)
end ) as "MN_L_12_Beams_Beam10"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '21+22' then
(case when try_cast(azimuth as double)+45 >=360 then (try_cast(azimuth as double)+45)-360
else try_cast(azimuth as double)+45 end)
end ) as "MN_L_12_Beams_Beam11"
--
,(case when antenna_type like 'Mini Lens-12 Beam' and map1_ports like '23+24' then
(case when try_cast(azimuth as double)+55 >=360 then (try_cast(azimuth as double)+55)-360
else try_cast(azimuth as double)+55 end)
end ) as "MN_L_12_Beams_Beam12"

from hive.thor_fw.v_cur_appian_cell_enrichment_all
--
where pow_status like 'Latest' and eutran_cell_id is not null and program_of_work not like '%DECOM%' and antenna_type is not null and antenna_type not like 'AIR5322' and antenna_type not like 'KP-23DOMNI-HV' and antenna_type not like 'TA-2x23243436-S11-90D')  a_db)"""

