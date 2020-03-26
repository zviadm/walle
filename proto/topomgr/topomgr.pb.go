// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: topomgr/topomgr.proto

package topomgr

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	empty "github.com/golang/protobuf/ptypes/empty"
	walleapi "github.com/zviadm/walle/proto/walleapi"
	grpc "google.golang.org/grpc"
	io "io"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type FetchTopologyRequest struct {
	TopologyUri          string   `protobuf:"bytes,1,opt,name=topology_uri,json=topologyUri,proto3" json:"topology_uri,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *FetchTopologyRequest) Reset()         { *m = FetchTopologyRequest{} }
func (m *FetchTopologyRequest) String() string { return proto.CompactTextString(m) }
func (*FetchTopologyRequest) ProtoMessage()    {}
func (*FetchTopologyRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_4b9802c6f2bebbad, []int{0}
}
func (m *FetchTopologyRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *FetchTopologyRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_FetchTopologyRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *FetchTopologyRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FetchTopologyRequest.Merge(m, src)
}
func (m *FetchTopologyRequest) XXX_Size() int {
	return m.Size()
}
func (m *FetchTopologyRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_FetchTopologyRequest.DiscardUnknown(m)
}

var xxx_messageInfo_FetchTopologyRequest proto.InternalMessageInfo

func (m *FetchTopologyRequest) GetTopologyUri() string {
	if m != nil {
		return m.TopologyUri
	}
	return ""
}

type UpdateTopologyRequest struct {
	TopologyUri          string             `protobuf:"bytes,1,opt,name=topology_uri,json=topologyUri,proto3" json:"topology_uri,omitempty"`
	Topology             *walleapi.Topology `protobuf:"bytes,2,opt,name=topology,proto3" json:"topology,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *UpdateTopologyRequest) Reset()         { *m = UpdateTopologyRequest{} }
func (m *UpdateTopologyRequest) String() string { return proto.CompactTextString(m) }
func (*UpdateTopologyRequest) ProtoMessage()    {}
func (*UpdateTopologyRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_4b9802c6f2bebbad, []int{1}
}
func (m *UpdateTopologyRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *UpdateTopologyRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_UpdateTopologyRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *UpdateTopologyRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_UpdateTopologyRequest.Merge(m, src)
}
func (m *UpdateTopologyRequest) XXX_Size() int {
	return m.Size()
}
func (m *UpdateTopologyRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_UpdateTopologyRequest.DiscardUnknown(m)
}

var xxx_messageInfo_UpdateTopologyRequest proto.InternalMessageInfo

func (m *UpdateTopologyRequest) GetTopologyUri() string {
	if m != nil {
		return m.TopologyUri
	}
	return ""
}

func (m *UpdateTopologyRequest) GetTopology() *walleapi.Topology {
	if m != nil {
		return m.Topology
	}
	return nil
}

type RegisterServerRequest struct {
	TopologyUri          string               `protobuf:"bytes,1,opt,name=topology_uri,json=topologyUri,proto3" json:"topology_uri,omitempty"`
	ServerId             string               `protobuf:"bytes,2,opt,name=server_id,json=serverId,proto3" json:"server_id,omitempty"`
	ServerInfo           *walleapi.ServerInfo `protobuf:"bytes,3,opt,name=server_info,json=serverInfo,proto3" json:"server_info,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *RegisterServerRequest) Reset()         { *m = RegisterServerRequest{} }
func (m *RegisterServerRequest) String() string { return proto.CompactTextString(m) }
func (*RegisterServerRequest) ProtoMessage()    {}
func (*RegisterServerRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_4b9802c6f2bebbad, []int{2}
}
func (m *RegisterServerRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RegisterServerRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RegisterServerRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *RegisterServerRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RegisterServerRequest.Merge(m, src)
}
func (m *RegisterServerRequest) XXX_Size() int {
	return m.Size()
}
func (m *RegisterServerRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_RegisterServerRequest.DiscardUnknown(m)
}

var xxx_messageInfo_RegisterServerRequest proto.InternalMessageInfo

func (m *RegisterServerRequest) GetTopologyUri() string {
	if m != nil {
		return m.TopologyUri
	}
	return ""
}

func (m *RegisterServerRequest) GetServerId() string {
	if m != nil {
		return m.ServerId
	}
	return ""
}

func (m *RegisterServerRequest) GetServerInfo() *walleapi.ServerInfo {
	if m != nil {
		return m.ServerInfo
	}
	return nil
}

func init() {
	proto.RegisterType((*FetchTopologyRequest)(nil), "FetchTopologyRequest")
	proto.RegisterType((*UpdateTopologyRequest)(nil), "UpdateTopologyRequest")
	proto.RegisterType((*RegisterServerRequest)(nil), "RegisterServerRequest")
}

func init() { proto.RegisterFile("topomgr/topomgr.proto", fileDescriptor_4b9802c6f2bebbad) }

var fileDescriptor_4b9802c6f2bebbad = []byte{
	// 341 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0x2d, 0xc9, 0x2f, 0xc8,
	0xcf, 0x4d, 0x2f, 0xd2, 0x87, 0xd2, 0x7a, 0x05, 0x45, 0xf9, 0x25, 0xf9, 0x52, 0xd2, 0xe9, 0xf9,
	0xf9, 0xe9, 0x39, 0xa9, 0xfa, 0x60, 0x5e, 0x52, 0x69, 0x9a, 0x7e, 0x6a, 0x6e, 0x41, 0x49, 0x25,
	0x54, 0x52, 0xbc, 0x3c, 0x31, 0x27, 0x27, 0x35, 0xb1, 0x20, 0x53, 0x1f, 0xc6, 0x80, 0x48, 0x28,
	0x59, 0x72, 0x89, 0xb8, 0xa5, 0x96, 0x24, 0x67, 0x84, 0xe4, 0x17, 0xe4, 0xe7, 0xe4, 0xa7, 0x57,
	0x06, 0xa5, 0x16, 0x96, 0xa6, 0x16, 0x97, 0x08, 0x29, 0x72, 0xf1, 0x94, 0x40, 0x85, 0xe2, 0x4b,
	0x8b, 0x32, 0x25, 0x18, 0x15, 0x18, 0x35, 0x38, 0x83, 0xb8, 0x61, 0x62, 0xa1, 0x45, 0x99, 0x4a,
	0x89, 0x5c, 0xa2, 0xa1, 0x05, 0x29, 0x89, 0x25, 0xa9, 0xa4, 0xeb, 0x15, 0x52, 0xe5, 0xe2, 0x80,
	0x71, 0x25, 0x98, 0x14, 0x18, 0x35, 0xb8, 0x8d, 0x38, 0xf5, 0xe0, 0xc6, 0xc0, 0xa5, 0x94, 0x5a,
	0x19, 0xb9, 0x44, 0x83, 0x52, 0xd3, 0x33, 0x8b, 0x4b, 0x52, 0x8b, 0x82, 0x53, 0x8b, 0xca, 0x52,
	0x8b, 0x48, 0xb0, 0x43, 0x9a, 0x8b, 0xb3, 0x18, 0xac, 0x27, 0x3e, 0x33, 0x05, 0x6c, 0x09, 0x67,
	0x10, 0x07, 0x44, 0xc0, 0x33, 0x45, 0x48, 0x87, 0x8b, 0x1b, 0x26, 0x99, 0x97, 0x96, 0x2f, 0xc1,
	0x0c, 0x76, 0x03, 0xb7, 0x1e, 0xc4, 0x12, 0xcf, 0xbc, 0xb4, 0xfc, 0x20, 0xae, 0x62, 0x38, 0xdb,
	0xe8, 0x14, 0x23, 0x17, 0x37, 0xc8, 0x79, 0xbe, 0x89, 0x79, 0x89, 0xe9, 0xa9, 0x45, 0x42, 0xc6,
	0x5c, 0xbc, 0x28, 0xa1, 0x26, 0x24, 0xaa, 0x87, 0x2d, 0x14, 0xa5, 0x10, 0x9e, 0x52, 0x62, 0x10,
	0x72, 0xe2, 0xe2, 0x43, 0x0d, 0x2f, 0x21, 0x31, 0x3d, 0xac, 0x01, 0x28, 0x25, 0xa6, 0x07, 0x89,
	0x4b, 0x3d, 0x58, 0x5c, 0xea, 0xb9, 0x82, 0xe2, 0x12, 0x62, 0x06, 0x6a, 0x78, 0x08, 0x89, 0xe9,
	0x61, 0x0d, 0x20, 0xdc, 0x66, 0x38, 0x99, 0x9f, 0x78, 0x24, 0xc7, 0x78, 0xe1, 0x91, 0x1c, 0xe3,
	0x83, 0x47, 0x72, 0x8c, 0x33, 0x1e, 0xcb, 0x31, 0x44, 0xa9, 0xa6, 0x67, 0x96, 0x64, 0x94, 0x26,
	0xe9, 0x25, 0xe7, 0xe7, 0xea, 0x57, 0x95, 0x65, 0x26, 0xa6, 0xe4, 0x42, 0x92, 0x09, 0x24, 0x2d,
	0xc1, 0xd2, 0x59, 0x12, 0x1b, 0x98, 0x6b, 0x0c, 0x08, 0x00, 0x00, 0xff, 0xff, 0xb9, 0x5a, 0x95,
	0xcc, 0x81, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// TopoManagerClient is the client API for TopoManager service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type TopoManagerClient interface {
	FetchTopology(ctx context.Context, in *FetchTopologyRequest, opts ...grpc.CallOption) (*walleapi.Topology, error)
	// TODO(zviad): This method may not be needed.
	UpdateTopology(ctx context.Context, in *UpdateTopologyRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	// Register new (serverId, serverAddr) pair in a specific topology.
	RegisterServer(ctx context.Context, in *RegisterServerRequest, opts ...grpc.CallOption) (*empty.Empty, error)
}

type topoManagerClient struct {
	cc *grpc.ClientConn
}

func NewTopoManagerClient(cc *grpc.ClientConn) TopoManagerClient {
	return &topoManagerClient{cc}
}

func (c *topoManagerClient) FetchTopology(ctx context.Context, in *FetchTopologyRequest, opts ...grpc.CallOption) (*walleapi.Topology, error) {
	out := new(walleapi.Topology)
	err := c.cc.Invoke(ctx, "/TopoManager/FetchTopology", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *topoManagerClient) UpdateTopology(ctx context.Context, in *UpdateTopologyRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/TopoManager/UpdateTopology", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *topoManagerClient) RegisterServer(ctx context.Context, in *RegisterServerRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/TopoManager/RegisterServer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TopoManagerServer is the server API for TopoManager service.
type TopoManagerServer interface {
	FetchTopology(context.Context, *FetchTopologyRequest) (*walleapi.Topology, error)
	// TODO(zviad): This method may not be needed.
	UpdateTopology(context.Context, *UpdateTopologyRequest) (*empty.Empty, error)
	// Register new (serverId, serverAddr) pair in a specific topology.
	RegisterServer(context.Context, *RegisterServerRequest) (*empty.Empty, error)
}

func RegisterTopoManagerServer(s *grpc.Server, srv TopoManagerServer) {
	s.RegisterService(&_TopoManager_serviceDesc, srv)
}

func _TopoManager_FetchTopology_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FetchTopologyRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TopoManagerServer).FetchTopology(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/TopoManager/FetchTopology",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TopoManagerServer).FetchTopology(ctx, req.(*FetchTopologyRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TopoManager_UpdateTopology_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateTopologyRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TopoManagerServer).UpdateTopology(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/TopoManager/UpdateTopology",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TopoManagerServer).UpdateTopology(ctx, req.(*UpdateTopologyRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TopoManager_RegisterServer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterServerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TopoManagerServer).RegisterServer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/TopoManager/RegisterServer",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TopoManagerServer).RegisterServer(ctx, req.(*RegisterServerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _TopoManager_serviceDesc = grpc.ServiceDesc{
	ServiceName: "TopoManager",
	HandlerType: (*TopoManagerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "FetchTopology",
			Handler:    _TopoManager_FetchTopology_Handler,
		},
		{
			MethodName: "UpdateTopology",
			Handler:    _TopoManager_UpdateTopology_Handler,
		},
		{
			MethodName: "RegisterServer",
			Handler:    _TopoManager_RegisterServer_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "topomgr/topomgr.proto",
}

func (m *FetchTopologyRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *FetchTopologyRequest) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.TopologyUri) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintTopomgr(dAtA, i, uint64(len(m.TopologyUri)))
		i += copy(dAtA[i:], m.TopologyUri)
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *UpdateTopologyRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *UpdateTopologyRequest) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.TopologyUri) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintTopomgr(dAtA, i, uint64(len(m.TopologyUri)))
		i += copy(dAtA[i:], m.TopologyUri)
	}
	if m.Topology != nil {
		dAtA[i] = 0x12
		i++
		i = encodeVarintTopomgr(dAtA, i, uint64(m.Topology.Size()))
		n1, err := m.Topology.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n1
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *RegisterServerRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RegisterServerRequest) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.TopologyUri) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintTopomgr(dAtA, i, uint64(len(m.TopologyUri)))
		i += copy(dAtA[i:], m.TopologyUri)
	}
	if len(m.ServerId) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintTopomgr(dAtA, i, uint64(len(m.ServerId)))
		i += copy(dAtA[i:], m.ServerId)
	}
	if m.ServerInfo != nil {
		dAtA[i] = 0x1a
		i++
		i = encodeVarintTopomgr(dAtA, i, uint64(m.ServerInfo.Size()))
		n2, err := m.ServerInfo.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n2
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func encodeVarintTopomgr(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *FetchTopologyRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.TopologyUri)
	if l > 0 {
		n += 1 + l + sovTopomgr(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *UpdateTopologyRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.TopologyUri)
	if l > 0 {
		n += 1 + l + sovTopomgr(uint64(l))
	}
	if m.Topology != nil {
		l = m.Topology.Size()
		n += 1 + l + sovTopomgr(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *RegisterServerRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.TopologyUri)
	if l > 0 {
		n += 1 + l + sovTopomgr(uint64(l))
	}
	l = len(m.ServerId)
	if l > 0 {
		n += 1 + l + sovTopomgr(uint64(l))
	}
	if m.ServerInfo != nil {
		l = m.ServerInfo.Size()
		n += 1 + l + sovTopomgr(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovTopomgr(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozTopomgr(x uint64) (n int) {
	return sovTopomgr(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *FetchTopologyRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTopomgr
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: FetchTopologyRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: FetchTopologyRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TopologyUri", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTopomgr
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTopomgr
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTopomgr
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.TopologyUri = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTopomgr(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTopomgr
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthTopomgr
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *UpdateTopologyRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTopomgr
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: UpdateTopologyRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: UpdateTopologyRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TopologyUri", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTopomgr
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTopomgr
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTopomgr
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.TopologyUri = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Topology", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTopomgr
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTopomgr
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTopomgr
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Topology == nil {
				m.Topology = &walleapi.Topology{}
			}
			if err := m.Topology.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTopomgr(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTopomgr
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthTopomgr
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *RegisterServerRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTopomgr
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RegisterServerRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RegisterServerRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TopologyUri", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTopomgr
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTopomgr
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTopomgr
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.TopologyUri = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ServerId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTopomgr
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTopomgr
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTopomgr
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.ServerId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ServerInfo", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTopomgr
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTopomgr
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTopomgr
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.ServerInfo == nil {
				m.ServerInfo = &walleapi.ServerInfo{}
			}
			if err := m.ServerInfo.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTopomgr(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTopomgr
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthTopomgr
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipTopomgr(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTopomgr
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTopomgr
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTopomgr
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthTopomgr
			}
			iNdEx += length
			if iNdEx < 0 {
				return 0, ErrInvalidLengthTopomgr
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowTopomgr
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipTopomgr(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
				if iNdEx < 0 {
					return 0, ErrInvalidLengthTopomgr
				}
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthTopomgr = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTopomgr   = fmt.Errorf("proto: integer overflow")
)