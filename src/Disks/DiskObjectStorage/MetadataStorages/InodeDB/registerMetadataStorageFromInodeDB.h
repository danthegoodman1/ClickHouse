#pragma once

namespace DB
{

class MetadataStorageFactory;

/// Registers `metadata_type` `inodedb`. Builds without gRPC register nothing.
void registerMetadataStorageFromInodeDB(MetadataStorageFactory & factory);

}
