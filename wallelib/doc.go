// wallelib provides client libraries to connect to walle servers
//
// Topology & Discovery
//
// Every WALLE deployment has a single root topology stream that contains information
// about all the other topologies that exist within a deployment. Each topology contains
// information about all the streams that a single cluster serves.
//
// Example:
//
// /topology/<deployment name> - contains topology & discovery information about all other topologies:
//		/topology/database/user
//		/topology/database/example
//		...
// /topology/database/user - contains topology & discovery information about all the
// streams assciated with the "user" database:
//		/database/user/partition1
//		/database/user/partition2
//		/database/user/partition3
//		...
// /topology/database/example - contains topology & discovery information about all the
// streams assciated with the "example" database:
//		/database/example/partition1
//		/database/example/partition2
//		/database/example/partition3
//		...
//
//
package wallelib
