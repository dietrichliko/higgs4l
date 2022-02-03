#include "ROOT/RVec.hxx"
#include "Math/Vector4D.h"

using namespace ROOT;
using RVecF = RVec<float>;
using RVecI = RVec<int>;
using RVecS = RVec<size_t>;

const auto z_mass = 91.2;

// Find best Z pairs from Leptons
RVecS FindZZ(const RVecF &pt, const RVecF &eta, const RVecF &phi, const RVecF &mass, const RVecI &charge)
{
    auto idx = Combinations(pt, 2);
    auto best_mass = -1.;
    size_t best_i1 = -1;
    size_t best_i2 = -1;

    for (size_t i = 0; i < idx[0].size(); i++)
    {
        const auto i1 = idx[0][i];
        const auto i2 = idx[1][i];

        if (charge[i1] == -charge[i2])
        {
            auto p1 = Math::PtEtaPhiMVector(pt[i1], eta[i1], phi[i1], mass[i1]);
            auto p2 = Math::PtEtaPhiMVector(pt[i2], eta[i2], phi[i2], mass[i2]);
            auto mass = (p1 + p2).M();
            if (abs(mass - z_mass) < abs(best_mass - z_mass))
            {
                best_mass = mass;
                best_i1 = i1;
                best_i2 = i2;
            }
        }
    }
    RVecS result;
    result.reserve(4);
    result.emplace_back(best_i1);
    result.emplace_back(best_i2);
    for (size_t i = 0; i < 4; i++)
    {
        if (i != best_i1 && i != best_i2)
        {
            result.emplace_back(i);
        }
    }
    return result;
}

// Compute mass of the two Lepton pairs
RVecF ZZInvMass(const RVecS &idx, const RVecF &pt, const RVecF &eta, const RVecF &phi, const RVecF &mass)
{
    RVecF masses;
    masses.reserve(2);

    auto i1 = idx[0];
    auto i2 = idx[1];
    auto p1 = Math::PtEtaPhiMVector(pt[i1], eta[i1], phi[i1], mass[i1]);
    auto p2 = Math::PtEtaPhiMVector(pt[i2], eta[i2], phi[i2], mass[i2]);
    masses.emplace_back((p1 + p2).M());

    i1 = idx[2];
    i2 = idx[3];
    p1 = Math::PtEtaPhiMVector(pt[i1], eta[i1], phi[i1], mass[i1]);
    p2 = Math::PtEtaPhiMVector(pt[i2], eta[i2], phi[i2], mass[i2]);
    masses.emplace_back((p1 + p2).M());

    return masses;
}

// Compute mass of Higgs from four leptons of the same kind
float HiggsInvMass(const RVecF &pt, const RVecF &eta, const RVecF &phi, const RVecF &mass)
{
    Math::PtEtaPhiMVector p1(pt[0], eta[0], phi[0], mass[0]);
    Math::PtEtaPhiMVector p2(pt[1], eta[1], phi[1], mass[1]);
    Math::PtEtaPhiMVector p3(pt[2], eta[2], phi[2], mass[2]);
    Math::PtEtaPhiMVector p4(pt[3], eta[3], phi[3], mass[3]);
    return (p1 + p2 + p3 + p4).M();
}