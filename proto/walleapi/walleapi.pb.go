// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: walleapi/walleapi.proto

package walleapi

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
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

type Entry struct {
	EntryId  int64  `protobuf:"varint,1,opt,name=entry_id,json=entryId,proto3" json:"entry_id,omitempty"`
	WriterId string `protobuf:"bytes,2,opt,name=writer_id,json=writerId,proto3" json:"writer_id,omitempty"`
	Data     []byte `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
	// checksum_md5 is a rolling md5 for all `data`.
	ChecksumMd5          []byte   `protobuf:"bytes,4,opt,name=checksum_md5,json=checksumMd5,proto3" json:"checksum_md5,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Entry) Reset()         { *m = Entry{} }
func (m *Entry) String() string { return proto.CompactTextString(m) }
func (*Entry) ProtoMessage()    {}
func (*Entry) Descriptor() ([]byte, []int) {
	return fileDescriptor_3b522378fad0fbf4, []int{0}
}
func (m *Entry) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Entry) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Entry.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Entry) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Entry.Merge(m, src)
}
func (m *Entry) XXX_Size() int {
	return m.Size()
}
func (m *Entry) XXX_DiscardUnknown() {
	xxx_messageInfo_Entry.DiscardUnknown(m)
}

var xxx_messageInfo_Entry proto.InternalMessageInfo

func (m *Entry) GetEntryId() int64 {
	if m != nil {
		return m.EntryId
	}
	return 0
}

func (m *Entry) GetWriterId() string {
	if m != nil {
		return m.WriterId
	}
	return ""
}

func (m *Entry) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Entry) GetChecksumMd5() []byte {
	if m != nil {
		return m.ChecksumMd5
	}
	return nil
}

type ClaimWriterRequest struct {
	StreamUri            string   `protobuf:"bytes,1,opt,name=stream_uri,json=streamUri,proto3" json:"stream_uri,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ClaimWriterRequest) Reset()         { *m = ClaimWriterRequest{} }
func (m *ClaimWriterRequest) String() string { return proto.CompactTextString(m) }
func (*ClaimWriterRequest) ProtoMessage()    {}
func (*ClaimWriterRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_3b522378fad0fbf4, []int{1}
}
func (m *ClaimWriterRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ClaimWriterRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ClaimWriterRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ClaimWriterRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ClaimWriterRequest.Merge(m, src)
}
func (m *ClaimWriterRequest) XXX_Size() int {
	return m.Size()
}
func (m *ClaimWriterRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ClaimWriterRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ClaimWriterRequest proto.InternalMessageInfo

func (m *ClaimWriterRequest) GetStreamUri() string {
	if m != nil {
		return m.StreamUri
	}
	return ""
}

type ClaimWriterResponse struct {
	WriterId             string   `protobuf:"bytes,1,opt,name=writer_id,json=writerId,proto3" json:"writer_id,omitempty"`
	LastEntry            *Entry   `protobuf:"bytes,4,opt,name=last_entry,json=lastEntry,proto3" json:"last_entry,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ClaimWriterResponse) Reset()         { *m = ClaimWriterResponse{} }
func (m *ClaimWriterResponse) String() string { return proto.CompactTextString(m) }
func (*ClaimWriterResponse) ProtoMessage()    {}
func (*ClaimWriterResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_3b522378fad0fbf4, []int{2}
}
func (m *ClaimWriterResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ClaimWriterResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ClaimWriterResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ClaimWriterResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ClaimWriterResponse.Merge(m, src)
}
func (m *ClaimWriterResponse) XXX_Size() int {
	return m.Size()
}
func (m *ClaimWriterResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ClaimWriterResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ClaimWriterResponse proto.InternalMessageInfo

func (m *ClaimWriterResponse) GetWriterId() string {
	if m != nil {
		return m.WriterId
	}
	return ""
}

func (m *ClaimWriterResponse) GetLastEntry() *Entry {
	if m != nil {
		return m.LastEntry
	}
	return nil
}

type PutEntryRequest struct {
	StreamUri            string   `protobuf:"bytes,1,opt,name=stream_uri,json=streamUri,proto3" json:"stream_uri,omitempty"`
	Entry                *Entry   `protobuf:"bytes,2,opt,name=entry,proto3" json:"entry,omitempty"`
	CommittedEntryId     int64    `protobuf:"varint,3,opt,name=committed_entry_id,json=committedEntryId,proto3" json:"committed_entry_id,omitempty"`
	CommittedEntryMd5    []byte   `protobuf:"bytes,4,opt,name=committed_entry_md5,json=committedEntryMd5,proto3" json:"committed_entry_md5,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PutEntryRequest) Reset()         { *m = PutEntryRequest{} }
func (m *PutEntryRequest) String() string { return proto.CompactTextString(m) }
func (*PutEntryRequest) ProtoMessage()    {}
func (*PutEntryRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_3b522378fad0fbf4, []int{3}
}
func (m *PutEntryRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PutEntryRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PutEntryRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PutEntryRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PutEntryRequest.Merge(m, src)
}
func (m *PutEntryRequest) XXX_Size() int {
	return m.Size()
}
func (m *PutEntryRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PutEntryRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PutEntryRequest proto.InternalMessageInfo

func (m *PutEntryRequest) GetStreamUri() string {
	if m != nil {
		return m.StreamUri
	}
	return ""
}

func (m *PutEntryRequest) GetEntry() *Entry {
	if m != nil {
		return m.Entry
	}
	return nil
}

func (m *PutEntryRequest) GetCommittedEntryId() int64 {
	if m != nil {
		return m.CommittedEntryId
	}
	return 0
}

func (m *PutEntryRequest) GetCommittedEntryMd5() []byte {
	if m != nil {
		return m.CommittedEntryMd5
	}
	return nil
}

type PutEntryResponse struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PutEntryResponse) Reset()         { *m = PutEntryResponse{} }
func (m *PutEntryResponse) String() string { return proto.CompactTextString(m) }
func (*PutEntryResponse) ProtoMessage()    {}
func (*PutEntryResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_3b522378fad0fbf4, []int{4}
}
func (m *PutEntryResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PutEntryResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PutEntryResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PutEntryResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PutEntryResponse.Merge(m, src)
}
func (m *PutEntryResponse) XXX_Size() int {
	return m.Size()
}
func (m *PutEntryResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_PutEntryResponse.DiscardUnknown(m)
}

var xxx_messageInfo_PutEntryResponse proto.InternalMessageInfo

func init() {
	proto.RegisterType((*Entry)(nil), "Entry")
	proto.RegisterType((*ClaimWriterRequest)(nil), "ClaimWriterRequest")
	proto.RegisterType((*ClaimWriterResponse)(nil), "ClaimWriterResponse")
	proto.RegisterType((*PutEntryRequest)(nil), "PutEntryRequest")
	proto.RegisterType((*PutEntryResponse)(nil), "PutEntryResponse")
}

func init() { proto.RegisterFile("walleapi/walleapi.proto", fileDescriptor_3b522378fad0fbf4) }

var fileDescriptor_3b522378fad0fbf4 = []byte{
	// 362 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x92, 0xcf, 0x4a, 0xeb, 0x40,
	0x14, 0xc6, 0x3b, 0xfd, 0x77, 0x93, 0xd3, 0xc2, 0x4d, 0x4f, 0x2f, 0xdc, 0x5a, 0x35, 0xd4, 0x80,
	0xd0, 0x85, 0x44, 0x6c, 0xe9, 0xc6, 0x9d, 0x4a, 0x17, 0x5d, 0x08, 0x12, 0x90, 0xe2, 0x2a, 0xc4,
	0xce, 0x80, 0x83, 0x49, 0x13, 0x27, 0x13, 0x4b, 0xdf, 0xc4, 0x87, 0xf0, 0x41, 0x5c, 0xfa, 0x08,
	0x52, 0x5f, 0x44, 0x32, 0xb1, 0xad, 0x89, 0x2e, 0xdc, 0x9d, 0xf9, 0xbe, 0xc9, 0x97, 0xef, 0xfc,
	0x12, 0xf8, 0xbf, 0xf0, 0x7c, 0x9f, 0x79, 0x11, 0x3f, 0x5e, 0x0f, 0x76, 0x24, 0x42, 0x19, 0x5a,
	0x8f, 0x50, 0x1b, 0xcf, 0xa5, 0x58, 0xe2, 0x0e, 0x68, 0x2c, 0x1d, 0x5c, 0x4e, 0x3b, 0xa4, 0x47,
	0xfa, 0x15, 0xe7, 0x8f, 0x3a, 0x4f, 0x28, 0xee, 0x82, 0xbe, 0x10, 0x5c, 0x32, 0x91, 0x7a, 0xe5,
	0x1e, 0xe9, 0xeb, 0x8e, 0x96, 0x09, 0x13, 0x8a, 0x08, 0x55, 0xea, 0x49, 0xaf, 0x53, 0xe9, 0x91,
	0x7e, 0xd3, 0x51, 0x33, 0x1e, 0x40, 0x73, 0x76, 0xc7, 0x66, 0xf7, 0x71, 0x12, 0xb8, 0x01, 0x1d,
	0x75, 0xaa, 0xca, 0x6b, 0xac, 0xb5, 0x4b, 0x3a, 0xb2, 0x86, 0x80, 0x17, 0xbe, 0xc7, 0x83, 0xa9,
	0xca, 0x71, 0xd8, 0x43, 0xc2, 0x62, 0x89, 0xfb, 0x00, 0xb1, 0x14, 0xcc, 0x0b, 0xdc, 0x44, 0x70,
	0x55, 0x43, 0x77, 0xf4, 0x4c, 0xb9, 0x16, 0xdc, 0xba, 0x81, 0x76, 0xee, 0xa1, 0x38, 0x0a, 0xe7,
	0x31, 0xcb, 0xf7, 0x23, 0x85, 0x7e, 0x87, 0x00, 0xbe, 0x17, 0x4b, 0x57, 0x2d, 0xa3, 0x9a, 0x34,
	0x06, 0x75, 0x5b, 0xed, 0xec, 0xe8, 0xa9, 0xa3, 0x46, 0xeb, 0x99, 0xc0, 0xdf, 0xab, 0x24, 0x3b,
	0xfc, 0xae, 0x0d, 0xee, 0x41, 0x2d, 0x0b, 0x2d, 0xe7, 0x42, 0x33, 0x11, 0x8f, 0x00, 0x67, 0x61,
	0x10, 0x70, 0x29, 0x19, 0x75, 0x37, 0x64, 0x2b, 0x8a, 0xac, 0xb1, 0x71, 0xc6, 0x9f, 0x88, 0x6d,
	0x68, 0x17, 0x6f, 0x6f, 0xc1, 0xb5, 0xf2, 0xd7, 0x53, 0x7c, 0x08, 0xc6, 0xb6, 0x6d, 0x86, 0x61,
	0xb0, 0x04, 0x6d, 0x9a, 0x7e, 0xdc, 0xb3, 0x88, 0xe3, 0x29, 0x34, 0xbe, 0x90, 0xc2, 0xb6, 0xfd,
	0x1d, 0x76, 0xf7, 0x9f, 0xfd, 0x03, 0x4c, 0xab, 0x84, 0x27, 0xa0, 0xad, 0xb3, 0xd1, 0xb0, 0x0b,
	0x50, 0xba, 0x2d, 0xbb, 0xf8, 0x62, 0xab, 0x74, 0x6e, 0xbc, 0xac, 0x4c, 0xf2, 0xba, 0x32, 0xc9,
	0xdb, 0xca, 0x24, 0x4f, 0xef, 0x66, 0xe9, 0xb6, 0xae, 0x7e, 0xaf, 0xe1, 0x47, 0x00, 0x00, 0x00,
	0xff, 0xff, 0xd2, 0x74, 0x24, 0xcd, 0x79, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// WalleApiClient is the client API for WalleApi service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type WalleApiClient interface {
	ClaimWriter(ctx context.Context, in *ClaimWriterRequest, opts ...grpc.CallOption) (*ClaimWriterResponse, error)
	PutEntry(ctx context.Context, in *PutEntryRequest, opts ...grpc.CallOption) (*PutEntryResponse, error)
}

type walleApiClient struct {
	cc *grpc.ClientConn
}

func NewWalleApiClient(cc *grpc.ClientConn) WalleApiClient {
	return &walleApiClient{cc}
}

func (c *walleApiClient) ClaimWriter(ctx context.Context, in *ClaimWriterRequest, opts ...grpc.CallOption) (*ClaimWriterResponse, error) {
	out := new(ClaimWriterResponse)
	err := c.cc.Invoke(ctx, "/WalleApi/ClaimWriter", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *walleApiClient) PutEntry(ctx context.Context, in *PutEntryRequest, opts ...grpc.CallOption) (*PutEntryResponse, error) {
	out := new(PutEntryResponse)
	err := c.cc.Invoke(ctx, "/WalleApi/PutEntry", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WalleApiServer is the server API for WalleApi service.
type WalleApiServer interface {
	ClaimWriter(context.Context, *ClaimWriterRequest) (*ClaimWriterResponse, error)
	PutEntry(context.Context, *PutEntryRequest) (*PutEntryResponse, error)
}

func RegisterWalleApiServer(s *grpc.Server, srv WalleApiServer) {
	s.RegisterService(&_WalleApi_serviceDesc, srv)
}

func _WalleApi_ClaimWriter_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClaimWriterRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WalleApiServer).ClaimWriter(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/WalleApi/ClaimWriter",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WalleApiServer).ClaimWriter(ctx, req.(*ClaimWriterRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WalleApi_PutEntry_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutEntryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WalleApiServer).PutEntry(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/WalleApi/PutEntry",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WalleApiServer).PutEntry(ctx, req.(*PutEntryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _WalleApi_serviceDesc = grpc.ServiceDesc{
	ServiceName: "WalleApi",
	HandlerType: (*WalleApiServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ClaimWriter",
			Handler:    _WalleApi_ClaimWriter_Handler,
		},
		{
			MethodName: "PutEntry",
			Handler:    _WalleApi_PutEntry_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "walleapi/walleapi.proto",
}

func (m *Entry) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Entry) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.EntryId != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(m.EntryId))
	}
	if len(m.WriterId) > 0 {
		dAtA[i] = 0x12
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(len(m.WriterId)))
		i += copy(dAtA[i:], m.WriterId)
	}
	if len(m.Data) > 0 {
		dAtA[i] = 0x1a
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(len(m.Data)))
		i += copy(dAtA[i:], m.Data)
	}
	if len(m.ChecksumMd5) > 0 {
		dAtA[i] = 0x22
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(len(m.ChecksumMd5)))
		i += copy(dAtA[i:], m.ChecksumMd5)
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *ClaimWriterRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ClaimWriterRequest) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.StreamUri) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(len(m.StreamUri)))
		i += copy(dAtA[i:], m.StreamUri)
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *ClaimWriterResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ClaimWriterResponse) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.WriterId) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(len(m.WriterId)))
		i += copy(dAtA[i:], m.WriterId)
	}
	if m.LastEntry != nil {
		dAtA[i] = 0x22
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(m.LastEntry.Size()))
		n1, err := m.LastEntry.MarshalTo(dAtA[i:])
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

func (m *PutEntryRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PutEntryRequest) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.StreamUri) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(len(m.StreamUri)))
		i += copy(dAtA[i:], m.StreamUri)
	}
	if m.Entry != nil {
		dAtA[i] = 0x12
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(m.Entry.Size()))
		n2, err := m.Entry.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n2
	}
	if m.CommittedEntryId != 0 {
		dAtA[i] = 0x18
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(m.CommittedEntryId))
	}
	if len(m.CommittedEntryMd5) > 0 {
		dAtA[i] = 0x22
		i++
		i = encodeVarintWalleapi(dAtA, i, uint64(len(m.CommittedEntryMd5)))
		i += copy(dAtA[i:], m.CommittedEntryMd5)
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *PutEntryResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PutEntryResponse) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func encodeVarintWalleapi(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *Entry) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.EntryId != 0 {
		n += 1 + sovWalleapi(uint64(m.EntryId))
	}
	l = len(m.WriterId)
	if l > 0 {
		n += 1 + l + sovWalleapi(uint64(l))
	}
	l = len(m.Data)
	if l > 0 {
		n += 1 + l + sovWalleapi(uint64(l))
	}
	l = len(m.ChecksumMd5)
	if l > 0 {
		n += 1 + l + sovWalleapi(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *ClaimWriterRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.StreamUri)
	if l > 0 {
		n += 1 + l + sovWalleapi(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *ClaimWriterResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.WriterId)
	if l > 0 {
		n += 1 + l + sovWalleapi(uint64(l))
	}
	if m.LastEntry != nil {
		l = m.LastEntry.Size()
		n += 1 + l + sovWalleapi(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *PutEntryRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.StreamUri)
	if l > 0 {
		n += 1 + l + sovWalleapi(uint64(l))
	}
	if m.Entry != nil {
		l = m.Entry.Size()
		n += 1 + l + sovWalleapi(uint64(l))
	}
	if m.CommittedEntryId != 0 {
		n += 1 + sovWalleapi(uint64(m.CommittedEntryId))
	}
	l = len(m.CommittedEntryMd5)
	if l > 0 {
		n += 1 + l + sovWalleapi(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *PutEntryResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovWalleapi(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozWalleapi(x uint64) (n int) {
	return sovWalleapi(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Entry) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowWalleapi
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
			return fmt.Errorf("proto: Entry: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Entry: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field EntryId", wireType)
			}
			m.EntryId = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.EntryId |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field WriterId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
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
				return ErrInvalidLengthWalleapi
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthWalleapi
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.WriterId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthWalleapi
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthWalleapi
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Data = append(m.Data[:0], dAtA[iNdEx:postIndex]...)
			if m.Data == nil {
				m.Data = []byte{}
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ChecksumMd5", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthWalleapi
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthWalleapi
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.ChecksumMd5 = append(m.ChecksumMd5[:0], dAtA[iNdEx:postIndex]...)
			if m.ChecksumMd5 == nil {
				m.ChecksumMd5 = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipWalleapi(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthWalleapi
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthWalleapi
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
func (m *ClaimWriterRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowWalleapi
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
			return fmt.Errorf("proto: ClaimWriterRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ClaimWriterRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StreamUri", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
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
				return ErrInvalidLengthWalleapi
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthWalleapi
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.StreamUri = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipWalleapi(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthWalleapi
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthWalleapi
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
func (m *ClaimWriterResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowWalleapi
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
			return fmt.Errorf("proto: ClaimWriterResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ClaimWriterResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field WriterId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
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
				return ErrInvalidLengthWalleapi
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthWalleapi
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.WriterId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field LastEntry", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
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
				return ErrInvalidLengthWalleapi
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthWalleapi
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.LastEntry == nil {
				m.LastEntry = &Entry{}
			}
			if err := m.LastEntry.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipWalleapi(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthWalleapi
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthWalleapi
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
func (m *PutEntryRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowWalleapi
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
			return fmt.Errorf("proto: PutEntryRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PutEntryRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StreamUri", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
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
				return ErrInvalidLengthWalleapi
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthWalleapi
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.StreamUri = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Entry", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
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
				return ErrInvalidLengthWalleapi
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthWalleapi
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Entry == nil {
				m.Entry = &Entry{}
			}
			if err := m.Entry.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field CommittedEntryId", wireType)
			}
			m.CommittedEntryId = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.CommittedEntryId |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CommittedEntryMd5", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWalleapi
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthWalleapi
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthWalleapi
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.CommittedEntryMd5 = append(m.CommittedEntryMd5[:0], dAtA[iNdEx:postIndex]...)
			if m.CommittedEntryMd5 == nil {
				m.CommittedEntryMd5 = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipWalleapi(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthWalleapi
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthWalleapi
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
func (m *PutEntryResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowWalleapi
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
			return fmt.Errorf("proto: PutEntryResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PutEntryResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipWalleapi(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthWalleapi
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthWalleapi
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
func skipWalleapi(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowWalleapi
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
					return 0, ErrIntOverflowWalleapi
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
					return 0, ErrIntOverflowWalleapi
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
				return 0, ErrInvalidLengthWalleapi
			}
			iNdEx += length
			if iNdEx < 0 {
				return 0, ErrInvalidLengthWalleapi
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowWalleapi
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
				next, err := skipWalleapi(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
				if iNdEx < 0 {
					return 0, ErrInvalidLengthWalleapi
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
	ErrInvalidLengthWalleapi = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowWalleapi   = fmt.Errorf("proto: integer overflow")
)
