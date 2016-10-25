/*
 *  Copyright 2009-2016 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.api.motan.cluster.loadbalance;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.weibo.api.motan.core.extension.SpiMeta;
import com.weibo.api.motan.rpc.Referer;
import com.weibo.api.motan.rpc.Request;
import com.weibo.api.motan.rpc.URL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@SpiMeta(name = "loadBalanceForSeqService")
public class SeqServiceLoadBalance<T> extends AbstractLoadBalance<T> {

    private ImmutableList<Referer<T>> serverRefererList;

    @Override
    public void onRefresh(List<Referer<T>> referers) {
        super.onRefresh(referers);

        List<Referer<T>> copyRefererList = new ArrayList<Referer<T>>(referers);
        Collections.sort(copyRefererList, new Comparator<Referer<T>>() {
            @Override
            public int compare(final Referer<T> refer1, final Referer<T> refer2) {
                return extractReferHostAndPort(refer1).compareTo(extractReferHostAndPort(refer2));
            }
        });
        serverRefererList = ImmutableList.copyOf(copyRefererList);
    }

    private String extractReferHostAndPort(final Referer<T> referer) {
        final URL url = referer.getServiceUrl();
        return url.getHost().concat(":").concat(Integer.toString(url.getPort()));
    }

    @Override
    protected Referer<T> doSelect(Request request) {
        checkRequest(request);

        final Long userId = (Long) request.getArguments()[0];
        final long seqSessionId = SeqSessionCalc.calcSeqSession(userId);
        final int referListSize = getReferers().size();
        final int index = Hashing.consistentHash(seqSessionId, referListSize);
        return serverRefererList.get(index);
    }

    private void checkRequest(final Request request) {
        final String interfaceName = request.getInterfaceName();
        if (!Objects.equals(interfaceName, "SeqGeneratorService")) {
            throw new IllegalArgumentException("this loadBalance can only be used for SeqGeneratorService");
        }
    }

    @Override
    protected void doSelectToHolder(Request request, List<Referer<T>> refersHolder) {
        checkRequest(request);

        final Long userId = (Long) request.getArguments()[0];
        final long seqSessionId = SeqSessionCalc.calcSeqSession(userId);
        final int referListSize = getReferers().size();
        final int index = Hashing.consistentHash(seqSessionId, referListSize);
        final Referer<T> referer = getReferers().get(index);
        if (referer.isAvailable()) {
            refersHolder.add(referer);
        }
    }
}
