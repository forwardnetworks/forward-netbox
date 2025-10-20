---
description: On this page, the concept of branches and how they help the NetBox plugin propose changes to the production database.
---

# Extras

## NetBox Cloud

There are some challenges when using the Forward Enterprise NetBox plugin with NetBox Cloud. The plugin is designed to run on the NetBox server which has reachability to a Forward Enterprise instance. To overcome this limitation, the plugin supports a NetBox Cloud data collection workflow. A companion Python script can gather data from Forward Enterprise and push the required snapshots and raw datasets into NetBox through the plugin's REST API. Once the raw data has been pushed up, the transformation process will run as normal against the existing source configuration.

Using the offline data push limits some functionality of the plugin such as:

- Device Tables

## Device Tables

Forward Enterprise encompasses an extensive array of configuration and operational state data for devices, not all of which can be synchronized into NetBox. The plugin offers a means to seamlessly view this data within NetBox by dynamically generating tables for each Forward Enterprise table, allowing the use of serial numbers as filters for output.

These tables will be available as a tab under all devices. If no serial number is available, users will be unable to perform a lookup as we require the serial number to filter the Forward Enterprise API data.

The default behavior in Forward Enterprise involves utilizing the `$last` snapshot to generate the table. However, users have the option to manually modify this by changing the **snapshot** form field. Additionally, the Forward Enterprise Source field allows specifying an alternative Forward Enterprise instance for the lookup; by default, the source last used to synchronize the device will be used.

The cache can be used to store the data for a given device, allowing for faster lookups. This is enabled by default but can be disabled by unchecking the **Cache** checkbox. The cache will be updated every time the Forward Enterprise API is queried. The cache is stored for 24 hours.

![Device Table](../images/user_guide/extras_device_table.png)

## Site Topology


If sites have been synced with the plugin and have the `Forward Enterprise Source` custom field set a button will apear with the ability to get a topology of the `$last` or `$prev` snapshot. The will communicate with the Forward Enterprise API to generate a topology (SVG) for the site showing the devices and links between them. You can also use the link provided below to open the topology directly in Forward Enterprise for further analysis and interaction.

![Site Topology](../images/user_guide/extras_site_topology.png)
