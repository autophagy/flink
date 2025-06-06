/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoUtils;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Compound meta information for a registered state in a keyed state backend. This combines all
 * serializers and the state name.
 *
 * @param <S> Type of state value
 */
public class RegisteredKeyValueStateBackendMetaInfo<N, S> extends RegisteredStateMetaInfoBase {

    @Nonnull protected final StateDescriptor.Type stateType;
    @Nonnull protected final StateSerializerProvider<N> namespaceSerializerProvider;
    @Nonnull protected final StateSerializerProvider<S> stateSerializerProvider;
    @Nonnull protected StateSnapshotTransformFactory<S> stateSnapshotTransformFactory;

    public RegisteredKeyValueStateBackendMetaInfo(
            @Nonnull String name,
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull TypeSerializer<S> stateSerializer) {

        this(
                name,
                stateType,
                StateSerializerProvider.fromNewRegisteredSerializer(namespaceSerializer),
                StateSerializerProvider.fromNewRegisteredSerializer(stateSerializer),
                StateSnapshotTransformFactory.noTransform());
    }

    public RegisteredKeyValueStateBackendMetaInfo(
            @Nonnull String name,
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull TypeSerializer<S> stateSerializer,
            @Nonnull StateSnapshotTransformFactory<S> stateSnapshotTransformFactory) {

        this(
                name,
                stateType,
                StateSerializerProvider.fromNewRegisteredSerializer(namespaceSerializer),
                StateSerializerProvider.fromNewRegisteredSerializer(stateSerializer),
                stateSnapshotTransformFactory);
    }

    @SuppressWarnings("unchecked")
    public RegisteredKeyValueStateBackendMetaInfo(@Nonnull StateMetaInfoSnapshot snapshot) {
        this(
                snapshot.getName(),
                StateDescriptor.Type.valueOf(
                        snapshot.getOption(
                                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE)),
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        (TypeSerializerSnapshot<N>)
                                Preconditions.checkNotNull(
                                        snapshot.getTypeSerializerSnapshot(
                                                StateMetaInfoSnapshot.CommonSerializerKeys
                                                        .NAMESPACE_SERIALIZER))),
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        (TypeSerializerSnapshot<S>)
                                Preconditions.checkNotNull(
                                        snapshot.getTypeSerializerSnapshot(
                                                StateMetaInfoSnapshot.CommonSerializerKeys
                                                        .VALUE_SERIALIZER))),
                StateSnapshotTransformFactory.noTransform());

        Preconditions.checkState(
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE_V2
                        == snapshot.getBackendStateType());
    }

    public RegisteredKeyValueStateBackendMetaInfo(
            @Nonnull String name,
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull StateSerializerProvider<N> namespaceSerializerProvider,
            @Nonnull StateSerializerProvider<S> stateSerializerProvider,
            @Nonnull StateSnapshotTransformFactory<S> stateSnapshotTransformFactory) {

        super(name);
        this.stateType = stateType;
        this.namespaceSerializerProvider = namespaceSerializerProvider;
        this.stateSerializerProvider = stateSerializerProvider;
        this.stateSnapshotTransformFactory = stateSnapshotTransformFactory;
    }

    @Nonnull
    public StateDescriptor.Type getStateType() {
        return stateType;
    }

    @Nonnull
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializerProvider.currentSchemaSerializer();
    }

    @Nonnull
    public TypeSerializer<S> getStateSerializer() {
        return stateSerializerProvider.currentSchemaSerializer();
    }

    @Nonnull
    public TypeSerializerSchemaCompatibility<S> updateStateSerializer(
            TypeSerializer<S> newStateSerializer) {
        return stateSerializerProvider.registerNewSerializerForRestoredState(newStateSerializer);
    }

    @Nonnull
    public TypeSerializerSchemaCompatibility<N> updateNamespaceSerializer(
            TypeSerializer<N> newNamespaceSerializer) {
        return namespaceSerializerProvider.registerNewSerializerForRestoredState(
                newNamespaceSerializer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegisteredKeyValueStateBackendMetaInfo<?, ?> that =
                (RegisteredKeyValueStateBackendMetaInfo<?, ?>) o;

        if (!stateType.equals(that.stateType)) {
            return false;
        }

        if (!getName().equals(that.getName())) {
            return false;
        }

        return getStateSerializer().equals(that.getStateSerializer())
                && getNamespaceSerializer().equals(that.getNamespaceSerializer());
    }

    @Override
    public String toString() {
        return "RegisteredKeyedBackendStateMetaInfo{"
                + "stateType="
                + stateType
                + ", name='"
                + name
                + '\''
                + ", namespaceSerializer="
                + getNamespaceSerializer()
                + ", stateSerializer="
                + getStateSerializer()
                + '}';
    }

    @Override
    public int hashCode() {
        int result = getName().hashCode();
        result = 31 * result + getStateType().hashCode();
        result = 31 * result + getNamespaceSerializer().hashCode();
        result = 31 * result + getStateSerializer().hashCode();
        return result;
    }

    @Nonnull
    @Override
    public StateMetaInfoSnapshot snapshot() {
        return computeSnapshot();
    }

    @Nonnull
    @Override
    public RegisteredKeyValueStateBackendMetaInfo<N, S> withSerializerUpgradesAllowed() {
        return new RegisteredKeyValueStateBackendMetaInfo<>(snapshot());
    }

    public void checkStateMetaInfo(StateDescriptor<?> stateDesc) {
        Preconditions.checkState(
                Objects.equals(stateDesc.getStateId(), getName()),
                "Incompatible state names. "
                        + "Was ["
                        + getName()
                        + "], "
                        + "registered with ["
                        + stateDesc.getStateId()
                        + "].");

        Preconditions.checkState(
                stateDesc.getType() == getStateType(),
                "Incompatible key/value state types. "
                        + "Was ["
                        + getStateType()
                        + "], "
                        + "registered with ["
                        + stateDesc.getType()
                        + "].");
    }

    @Nonnull
    private StateMetaInfoSnapshot computeSnapshot() {
        Map<String, String> optionsMap =
                Collections.singletonMap(
                        StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
                        stateType.toString());

        Map<String, TypeSerializer<?>> serializerMap = CollectionUtil.newHashMapWithExpectedSize(2);
        Map<String, TypeSerializerSnapshot<?>> serializerConfigSnapshotsMap =
                CollectionUtil.newHashMapWithExpectedSize(2);

        String namespaceSerializerKey =
                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString();
        String valueSerializerKey =
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString();

        TypeSerializer<N> namespaceSerializer = getNamespaceSerializer();
        serializerMap.put(namespaceSerializerKey, namespaceSerializer.duplicate());
        serializerConfigSnapshotsMap.put(
                namespaceSerializerKey, namespaceSerializer.snapshotConfiguration());

        TypeSerializer<S> stateSerializer = getStateSerializer();
        serializerMap.put(valueSerializerKey, stateSerializer.duplicate());
        serializerConfigSnapshotsMap.put(
                valueSerializerKey, stateSerializer.snapshotConfiguration());

        return new StateMetaInfoSnapshot(
                name,
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE_V2,
                optionsMap,
                serializerConfigSnapshotsMap,
                serializerMap);
    }

    public static RegisteredStateMetaInfoBase fromMetaInfoSnapshot(
            @Nonnull StateMetaInfoSnapshot snapshot) {

        final StateMetaInfoSnapshot.BackendStateType backendStateType =
                snapshot.getBackendStateType();
        switch (backendStateType) {
            case KEY_VALUE:
                return RegisteredStateMetaInfoUtils.createMetaInfoV2FromV1Snapshot(snapshot);
            case KEY_VALUE_V2:
                if (snapshot.getOption(
                                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString())
                        .equals(StateDescriptor.Type.MAP.toString())) {
                    return new RegisteredKeyAndUserKeyValueStateBackendMetaInfo<>(snapshot);
                } else {
                    return new RegisteredKeyValueStateBackendMetaInfo<>(snapshot);
                }
            default:
                return RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(snapshot);
        }
    }
}
