package io.microstack.sloth.rpc;

public interface SlothStubPool {

    SlothStub getByEndpoint(String endpoint);

}
